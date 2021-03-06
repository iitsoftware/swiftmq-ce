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

package com.swiftmq.impl.jms.standard.v500;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v500.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.collection.RingBuffer;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

public class TransactedQueueSession extends TransactedSession {
    BrowserManager browserManager;
    EntityList senderEntityList = null;
    EntityList receiverEntityList = null;

    public TransactedQueueSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin) {
        super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin);
        browserManager = new BrowserManager(ctx);
        if (ctx.sessionEntity != null) {
            senderEntityList = (EntityList) sessionEntity.getEntity("sender");
            receiverEntityList = (EntityList) sessionEntity.getEntity("receiver");
        }
    }

    public void visitCommitRequest(CommitRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCommitRequest");
        CommitReply reply = (CommitReply) req.createReply();
        reply.setOk(true);
        try {
            // first: produce all messages
            Object[] wrapper = req.getMessages();
            ctx.incMsgsSent(wrapper.length);
            req.setMessages(null);
            long fcDelay = 0;
            RingBuffer tempProducers = null;
            for (int i = 0; i < wrapper.length; i++) {
                DataByteArrayInputStream dis = new DataByteArrayInputStream((byte[]) wrapper[i]);
                int producerId = dis.readInt();
                int type = dis.readInt();
                MessageImpl msg = MessageImpl.createInstance(type);
                msg.readContent(dis);
                dis.close();
                Producer producer = null;
                if (producerId == -1) {
                    String queueName = ((QueueImpl) msg.getJMSDestination()).getQueueName();
                    if (!ctx.queueManager.isQueueRunning(queueName))
                        throw new InvalidDestinationException("Invalid destination: " + queueName);
                    producer = new QueueProducer(ctx, queueName);
                    if (tempProducers == null)
                        tempProducers = new RingBuffer(8);
                    tempProducers.add(producer);
                    transactionManager.addTransactionFactory(producer);
                } else {
                    producer = (Producer) producerList.get(producerId);
                }
                QueuePushTransaction transaction = (QueuePushTransaction) producer.getTransaction();
                transaction.putMessage(msg);
                fcDelay = Math.max(fcDelay, transaction.getFlowControlDelay());
                if (producerId == -1)
                    producer.markForClose();
            }
            // Next: do the commit
            transactionManager.commit();
            if (tempProducers != null) {
                int size = tempProducers.getSize();
                for (int i = 0; i < size; i++) {
                    ((Producer) tempProducers.remove()).close();
                }
            }
            reply.setDelay(fcDelay);
            purgeMarkedProducers();
            purgeMarkedConsumers();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/commit produced messages failed: " + e.getMessage());
            reply.setOk(false);
            reply.setException((e instanceof JMSException) ? e : new javax.jms.JMSException(e.toString()));
        }
        reply.send();
    }

    public void visitCreateProducerRequest(CreateProducerRequest req) {
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
            int producerId;
            QueueProducer producer;
            producerId = ArrayListTool.setFirstFreeOrExpand(producerList, null);
            producer = new QueueProducer(ctx, queue.getQueueName());
            producerList.set(producerId, producer);
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

            // enlist it at the transaction manager
            transactionManager.addTransactionFactory(producer);

        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
            ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
            reply.setOk(false);
            reply.setException((e instanceof JMSException) ? e : new javax.jms.JMSException(e.toString()));
            ctx.activeLogin.getResourceLimitGroup().decProducers();
        }
        reply.send();
    }

    public void visitCloseProducerRequest(CloseProducerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseProducerRequest");
        ctx.activeLogin.getResourceLimitGroup().decProducers();
        QueueProducer producer = null;
        producer = (QueueProducer) producerList.get(req.getQueueProducerId());

        // mark for close and close it later after commit/rollback
        producer.markForClose();

        if (ctx.sessionEntity != null)
            senderEntityList.removeDynamicEntity(producer);
        CloseProducerReply reply = (CloseProducerReply) req.createReply();
        reply.setOk(true);
        reply.send();
    }

    public void visitCreateConsumerRequest(CreateConsumerRequest req) {
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
            if (!queueName.endsWith('@' + SwiftletManager.getInstance().getRouterName()))
                throw new InvalidDestinationException("Queue '" + queueName + "' is not local! Can't create a Consumer on it!");
            int consumerId = 0;
            QueueConsumer consumer = null;
            consumerId = ArrayListTool.setFirstFreeOrExpand(consumerList, null);
            consumer = new QueueConsumer(ctx, queueName, messageSelector);
            consumerList.set(consumerId, consumer);
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

            consumer.createReadTransaction();
            // enlist it at the transaction manager
            transactionManager.addTransactionFactory(consumer);
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
            reply.setException((e1 instanceof JMSException) ? e1 : new javax.jms.JMSException(e1.toString()));
        }
        reply.send();
    }

    public void visitCloseConsumerRequest(CloseConsumerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest");
        CloseConsumerReply reply = (CloseConsumerReply) req.createReply();
        int qcId = req.getQueueConsumerId();
        QueueConsumer consumer = (QueueConsumer) consumerList.get(qcId);

        // mark for close and close it later after commit/rollback
        consumer.markForClose();

        if (receiverEntityList != null)
            receiverEntityList.removeDynamicEntity(consumer);
        ctx.activeLogin.getResourceLimitGroup().decConsumers();
        reply.setOk(true);
        reply.send();
    }

    public void visitCreateBrowserRequest(CreateBrowserRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateBrowserRequest");
        browserManager.createBrowser(req);
    }

    public void visitFetchBrowserMessageRequest(FetchBrowserMessageRequest req) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
        browserManager.fetchBrowserMessage(req);
    }

    public void visitCloseBrowserRequest(CloseBrowserRequest req) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
        browserManager.closeBrowser(req);
    }

    protected void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
        super.close();
        browserManager.close();
        transactionManager.close();
    }

    public String toString() {
        return "TransactedQueueSession, dispatchId=" + dispatchId;
    }
}

