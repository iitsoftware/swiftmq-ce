/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v750.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

public class NontransactedQueueSession extends NontransactedSession {
    BrowserManager browserManager = null;
    EntityList senderEntityList = null;
    EntityList receiverEntityList = null;

    public NontransactedQueueSession(String connectionTracePrefix, Entity sessionEntity, EventLoop outboundLoop, int dispatchId, ActiveLogin activeLogin, int ackMode) {
        super(connectionTracePrefix, sessionEntity, outboundLoop, dispatchId, activeLogin, ackMode);
        browserManager = new BrowserManager(ctx);
        if (ctx.sessionEntity != null) {
            senderEntityList = (EntityList) sessionEntity.getEntity("sender");
            receiverEntityList = (EntityList) sessionEntity.getEntity("receiver");
        }
    }

    public void visit(ProduceMessageRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitProduceMessageRequest");
        ctx.incMsgsSent(1);
        ProduceMessageReply reply = null;
        if (req.isReplyRequired())
            reply = (ProduceMessageReply) req.createReply();
        int producerId = req.getQueueProducerId();
        Producer producer = null;
        try {
            MessageImpl msg = SMQPUtil.getMessage(req);
            long ttl = msg.getJMSExpiration();
            if (ttl > 0)
                msg.setJMSExpiration(System.currentTimeMillis() + ttl);
            if (producerId == -1) {
                String queueName = ((QueueImpl) msg.getJMSDestination()).getQueueName();
                if (!ctx.queueManager.isQueueRunning(queueName))
                    throw new InvalidDestinationException("Invalid destination: " + queueName);
                producer = new QueueProducer(ctx, queueName);
            } else {
                producer = (Producer) producerList.get(producerId);
            }
            QueuePushTransaction transaction = (QueuePushTransaction) producer.createTransaction();
            transaction.putMessage(msg);
            transaction.commit(new ProduceMessageCallback(producerId == -1 ? producer : null, reply));
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/produce messages failed: " + e.getMessage());
            if (req.isReplyRequired()) {
                reply.setOk(false);
                reply.setException(e);
                reply.send();
            }
        }
    }

    private class ProduceMessageCallback extends AsyncCompletionCallback {
        Producer producer = null;
        ProduceMessageReply reply = null;

        private ProduceMessageCallback(Producer producer, ProduceMessageReply reply) {
            this.producer = producer;
            this.reply = reply;
        }

        public void done(boolean success) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitProduceMessageRequest, ProduceMessageCallback.done, success=" + success);
            try {
                if (producer != null)
                    producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (reply != null) {
                if (success) {
                    Long delay = (Long) getResult();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitProduceMessageRequest, ProduceMessageCallback.done, delay=" + delay);
                    if (delay != null)
                        reply.setDelay(delay);
                    reply.setOk(true);
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitProduceMessageRequest, ProduceMessageCallback.done, exception=" + getException());
                    reply.setException(getException());
                    reply.setOk(false);
                }
                reply.send();
            }
        }
    }

    public void visit(CreateProducerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateProducerRequest");
        CreateProducerReply reply = (CreateProducerReply) req.createReply();
        try {
            ctx.activeLogin.getResourceLimitGroup().incProducers();
        } catch (ResourceLimitException e) {
            reply.setOk(false);
            reply.setException(new JMSException(e.toString()));
            reply.send();
            return;
        }
        QueueImpl queue = req.getQueue();
        try {
            if (!ctx.queueManager.isQueueRunning(queue.getQueueName()))
                throw new InvalidDestinationException("Invalid destination: " + queue.getQueueName());
            int producerId;
            QueueProducer producer = new QueueProducer(ctx, queue.getQueueName());
            producerId = producerList.add(producer);
            reply.setQueueProducerId(producerId);
            reply.setOk(true);
            if (senderEntityList != null) {
                Entity senderEntity = senderEntityList.createEntity();
                senderEntity.setName(queue.getQueueName() + "-" + producerId);
                senderEntity.setDynamicObject(producer);
                senderEntity.createCommands();
                Property prop = senderEntity.getProperty("queue");
                prop.setValue(queue.getQueueName());
                prop.setReadOnly(true);
                senderEntityList.addEntity(senderEntity);
            }
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
            ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
            reply.setOk(false);
            reply.setException(e);
            ctx.activeLogin.getResourceLimitGroup().decProducers();
        }
        reply.send();
    }

    public void visit(CloseProducerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseProducerRequest");
        ctx.activeLogin.getResourceLimitGroup().decProducers();
        if (req.getQueueProducerId() < producerList.size()) {
            QueueProducer producer = (QueueProducer) producerList.get(req.getQueueProducerId());
            producerList.remove(req.getQueueProducerId());
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception ignored) {
                }
                if (ctx.sessionEntity != null)
                    senderEntityList.removeDynamicEntity(producer);
            }
        }
        CloseProducerReply reply = (CloseProducerReply) req.createReply();
        reply.setOk(true);
        reply.send();
    }

    public void visit(CreateConsumerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateConsumerRequest");
        CreateConsumerReply reply = (CreateConsumerReply) req.createReply();
        try {
            ctx.activeLogin.getResourceLimitGroup().incConsumers();
        } catch (ResourceLimitException e) {
            reply.setOk(false);
            reply.setException(new JMSException(e.toString()));
            reply.send();
            return;
        }
        QueueImpl queue = req.getQueue();
        String messageSelector = req.getMessageSelector();
        String queueName = null;
        try {
            queueName = queue.getQueueName();
        } catch (JMSException ignored) {
        }
        try {
            queueName = validateDestination(queueName);
            int consumerId = 0;
            QueueConsumer consumer = null;
            consumer = new QueueConsumer(ctx, queueName, messageSelector);
            consumerId = consumerList.add(consumer);
            consumer.createReadTransaction();
            consumer.createTransaction();
            reply.setOk(true);
            reply.setQueueConsumerId(consumerId);
            if (receiverEntityList != null) {
                Entity consEntity = receiverEntityList.createEntity();
                consEntity.setName(queueName + "-" + consumerId);
                consEntity.setDynamicObject(consumer);
                consEntity.createCommands();
                Property prop = consEntity.getProperty("queue");
                prop.setValue(queueName);
                prop.setReadOnly(true);
                prop = consEntity.getProperty("selector");
                if (messageSelector != null) {
                    prop.setValue(messageSelector);
                }
                prop.setReadOnly(true);
                receiverEntityList.addEntity(consEntity);
            }
        } catch (InvalidSelectorException e) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateConsumer has invalid Selector: " + e);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateConsumer has invalid Selector: " + e);
            reply.setOk(false);
            reply.setException(e);
        } catch (Exception e1) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create consumer: " + e1);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create consumer: " + e1);
            reply.setOk(false);
            reply.setException(e1);
        }
        reply.send();
    }

    public void visit(CloseConsumerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest");
        CloseConsumerReply reply = (CloseConsumerReply) req.createReply();
        int qcId = req.getQueueConsumerId();
        QueueConsumer consumer = (QueueConsumer) consumerList.get(qcId);
        consumerList.remove(qcId);
        try {
            consumer.close();
        } catch (Exception ignored) {
        }
        if (receiverEntityList != null)
            receiverEntityList.removeDynamicEntity(consumer);
        ctx.activeLogin.getResourceLimitGroup().decConsumers();
        reply.setOk(true);
        reply.send();
    }

    public void visit(CreateBrowserRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateBrowserRequest");
        browserManager.createBrowser(req);
    }

    public void visit(FetchBrowserMessageRequest req) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
        browserManager.fetchBrowserMessage(req);
    }

    public void visit(CloseBrowserRequest req) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
        browserManager.closeBrowser(req);
    }

    protected void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
        super.close();
        browserManager.close();
    }

    public String toString() {
        return "NontransactedQueueSession, dispatchId=" + dispatchId;
    }
}

