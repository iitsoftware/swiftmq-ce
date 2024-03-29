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

import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.v750.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

public class NontransactedUnifiedSession extends NontransactedSession {
    BrowserManager browserManager = null;
    EntityList senderEntityList = null;
    EntityList receiverEntityList = null;
    EntityList publisherEntityList = null;
    EntityList subscriberEntityList = null;
    EntityList durableEntityList = null;

    public NontransactedUnifiedSession(String connectionTracePrefix, Entity sessionEntity, EventLoop outboundLoop, int dispatchId, ActiveLogin activeLogin, int ackMode) {
        super(connectionTracePrefix, sessionEntity, outboundLoop, dispatchId, activeLogin, ackMode);
        browserManager = new BrowserManager(ctx);
        if (ctx.sessionEntity != null) {
            senderEntityList = (EntityList) sessionEntity.getEntity("sender");
            receiverEntityList = (EntityList) sessionEntity.getEntity("receiver");
            publisherEntityList = (EntityList) sessionEntity.getEntity("publisher");
            subscriberEntityList = (EntityList) sessionEntity.getEntity("subscriber");
            durableEntityList = (EntityList) sessionEntity.getEntity("durable");
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
                DestinationImpl destImpl = (DestinationImpl) msg.getJMSDestination();
                switch (destImpl.getType()) {
                    case DestinationFactory.TYPE_QUEUE:
                    case DestinationFactory.TYPE_TEMPQUEUE:
                        String queueName = ((QueueImpl) destImpl).getQueueName();
                        if (!ctx.queueManager.isQueueRunning(queueName))
                            throw new InvalidDestinationException("Invalid destination: " + queueName);
                        producer = new QueueProducer(ctx, queueName);
                        break;
                    case DestinationFactory.TYPE_TOPIC:
                    case DestinationFactory.TYPE_TEMPTOPIC:
                        TopicImpl topic = (TopicImpl) msg.getJMSDestination();
                        if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
                            ctx.authSwiftlet.verifyTopicSenderSubscription(topic.getTopicName(), ctx.activeLogin.getLoginId());
                        producer = new TopicProducer(ctx, topic);
                        break;
                }
            } else {
                producer = producerList.get(producerId);
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

    public void visit(CreatePublisherRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreatePublisherRequest");
        CreatePublisherReply reply = (CreatePublisherReply) req.createReply();
        try {
            ctx.activeLogin.getResourceLimitGroup().incProducers();
        } catch (ResourceLimitException e) {
            reply.setOk(false);
            reply.setException(new JMSException(e.toString()));
            reply.send();
            return;
        }
        TopicImpl topic = req.getTopic();
        try {
            if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
                ctx.authSwiftlet.verifyTopicSenderSubscription(topic.getTopicName(), ctx.activeLogin.getLoginId());
            int producerId;
            TopicProducer producer = new TopicProducer(ctx, topic);
            producerId = producerList.add(producer);
            reply.setTopicPublisherId(producerId);
            reply.setOk(true);
            if (publisherEntityList != null) {
                Entity publisherEntity = publisherEntityList.createEntity();
                publisherEntity.setName(topic.getTopicName() + "-" + producerId);
                publisherEntity.setDynamicObject(producer);
                publisherEntity.createCommands();
                Property prop = publisherEntity.getProperty("topic");
                prop.setReadOnly(false);
                prop.setValue(topic.getTopicName());
                prop.setReadOnly(true);
                publisherEntityList.addEntity(publisherEntity);
            }
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
            ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
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
            Producer producer = (Producer) producerList.get(req.getQueueProducerId());
            producerList.remove(req.getQueueProducerId());
            try {
                producer.close();
            } catch (Exception ignored) {
            }
            // May be null for temp queues after a failover
            if (producer != null) {
                if (producer instanceof QueueProducer) {
                    if (ctx.sessionEntity != null)
                        senderEntityList.removeDynamicEntity(producer);
                } else {
                    if (ctx.sessionEntity != null)
                        publisherEntityList.removeDynamicEntity(producer);
                }
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
            QueueConsumer consumer = new QueueConsumer(ctx, queueName, messageSelector);
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

    public void visit(CreateSubscriberRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateSubscriberRequest");
        CreateSubscriberReply reply = (CreateSubscriberReply) req.createReply();
        try {
            ctx.activeLogin.getResourceLimitGroup().incConsumers();
        } catch (ResourceLimitException e) {
            reply.setOk(false);
            reply.setException(new JMSException(e.toString()));
            reply.send();
            return;
        }
        TopicImpl topic = req.getTopic();
        String messageSelector = req.getMessageSelector();
        boolean noLocal = req.isNoLocal();
        try {
            Entity subEntity = null;
            if (subscriberEntityList != null)
                subEntity = subscriberEntityList.createEntity();

            int consumerId = 0;
            TopicConsumer consumer = null;
            if (topic.getType() == DestinationFactory.TYPE_TOPIC) {
                consumer = new TopicConsumer(ctx, topic, messageSelector, noLocal);
                consumerId = consumerList.add(consumer);
                if (subEntity != null) {
                    Property prop = subEntity.getProperty("topic");
                    prop.setReadOnly(false);
                    prop.setValue(topic.getTopicName());
                    prop.setReadOnly(true);
                    prop = subEntity.getProperty("boundto");
                    prop.setReadOnly(false);
                    prop.setValue(topic.getQueueName());
                    prop.setReadOnly(true);
                    subEntity.setDynamicObject(consumer);
                }
                if (subEntity != null)
                    subEntity.setName(topic.getTopicName() + "-" + consumerId);
            } else {
                consumer = new TopicConsumer(ctx, topic, messageSelector, noLocal);
                consumerId = consumerList.add(consumer);
                if (subEntity != null)
                    subEntity.setDynamicObject(consumer);
                if (subEntity != null) {
                    subEntity.setName(topic.getQueueName() + "-" + consumerId);
                    Property prop = subEntity.getProperty("temptopic");
                    prop.setReadOnly(false);
                    prop.setValue(true);
                    prop.setReadOnly(true);
                    prop = subEntity.getProperty("boundto");
                    prop.setReadOnly(false);
                    prop.setValue(topic.getQueueName());
                    prop.setReadOnly(true);
                }
            }
            consumer.createReadTransaction();
            consumer.createTransaction();
            reply.setOk(true);
            reply.setTopicSubscriberId(consumerId);
            reply.setTmpQueueName(consumer.getQueueName());

            if (subEntity != null) {
                Property prop = subEntity.getProperty("nolocal");
                prop.setReadOnly(false);
                prop.setValue(noLocal);
                prop.setReadOnly(true);
                subEntity.createCommands();
                prop = subEntity.getProperty("selector");
                if (messageSelector != null) {
                    prop.setValue(messageSelector);
                }
                prop.setReadOnly(true);
                subscriberEntityList.addEntity(subEntity);
            }
        } catch (InvalidSelectorException e) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateSubscriber has invalid Selector: " + e);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateSubscriber has invalid Selector: " + e);
            reply.setOk(false);
            reply.setException(e);
        } catch (Exception e1) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create subscriber: " + e1);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create subscriber: " + e1);
            reply.setOk(false);
            reply.setException(e1);
        }
        reply.send();
    }

    public void visit(CloseConsumerRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest");
        CloseConsumerReply reply = (CloseConsumerReply) req.createReply();
        int qcId = req.getQueueConsumerId();
        Consumer consumer = consumerList.get(qcId);
        consumerList.remove(qcId);
        try {
            consumer.close();
        } catch (Exception ignored) {
        }
        if (consumer instanceof QueueConsumer) {
            if (receiverEntityList != null)
                receiverEntityList.removeDynamicEntity(consumer);
        } else {
            if (subscriberEntityList != null)
                subscriberEntityList.removeDynamicEntity(consumer);
        }
        ctx.activeLogin.getResourceLimitGroup().decConsumers();
        reply.setOk(true);
        reply.send();
    }

    public void visit(CreateDurableRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateDurableRequest");
        CreateDurableReply reply = (CreateDurableReply) req.createReply();
        try {
            ctx.activeLogin.getResourceLimitGroup().incConsumers();
        } catch (ResourceLimitException e) {
            reply.setOk(false);
            reply.setException(new JMSException(e.toString()));
            reply.send();
            return;
        }
        TopicImpl topic = req.getTopic();
        String messageSelector = req.getMessageSelector();
        boolean noLocal = req.isNoLocal();
        String durableName = req.getDurableName();
        try {
            int consumerId = 0;
            TopicDurableConsumer consumer = new TopicDurableConsumer(ctx, durableName, topic, messageSelector, noLocal);
            consumerId = consumerList.add(consumer);
            consumer.createReadTransaction();
            consumer.createTransaction();
            reply.setOk(true);
            reply.setTopicSubscriberId(consumerId);
            reply.setQueueName(consumer.getQueueName() + '@' + SwiftletManager.getInstance().getRouterName());

            if (durableEntityList != null) {
                Entity durEntity = durableEntityList.createEntity();
                durEntity.setName(ctx.activeLogin.getClientId() + "$" + durableName);
                durEntity.createCommands();
                Property prop = durEntity.getProperty("clientid");
                prop.setValue(ctx.activeLogin.getClientId());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("durablename");
                prop.setValue(durableName);
                prop.setReadOnly(true);
                prop = durEntity.getProperty("topic");
                prop.setValue(topic.getTopicName());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("boundto");
                prop.setValue(consumer.getQueueName());
                prop.setReadOnly(true);
                prop = durEntity.getProperty("nolocal");
                prop.setValue(noLocal);
                prop.setReadOnly(true);
                prop = durEntity.getProperty("selector");
                if (messageSelector != null) {
                    prop.setValue(messageSelector);
                }
                prop.setReadOnly(true);
                durableEntityList.addEntity(durEntity);
            }
        } catch (InvalidSelectorException e) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateDurable has invalid Selector: " + e);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateDurable has invalid Selector: " + e);
            reply.setOk(false);
            reply.setException(e);
        } catch (Exception e1) {
            ctx.activeLogin.getResourceLimitGroup().decConsumers();
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create durable: " + e1);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create durable: " + e1);
            reply.setOk(false);
            reply.setException(e1);
        }
        reply.send();
    }

    // FIXIT: Check for existing consumer on it and throw exception appropriate
    public void visit(DeleteDurableRequest req) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitDeleteDurableRequest");
        DeleteDurableReply reply = (DeleteDurableReply) req.createReply();
        String durableName = req.getDurableName();
        try {
            ctx.topicManager.deleteDurable(durableName, ctx.activeLogin);
            reply.setOk(true);
            if (durableEntityList != null)
                durableEntityList.removeEntity(durableEntityList.getEntity(ctx.activeLogin.getClientId() + "$" + durableName));
        } catch (Exception e1) {
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during delete durable: " + e1);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during delete durable: " + e1);
            reply.setOk(false);
            reply.setException(e1);
        }
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
        return "NontransactedUnifiedSession, dispatchId=" + dispatchId;
    }
}

