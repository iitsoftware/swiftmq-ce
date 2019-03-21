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

package com.swiftmq.impl.jms.standard.v630;

import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.v630.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.*;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.queue.SingleProcessorQueue;

import javax.jms.*;

public class NontransactedTopicSession extends NontransactedSession
{
  EntityList publisherEntityList = null;
  EntityList subscriberEntityList = null;
  EntityList durableEntityList = null;

  public NontransactedTopicSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin, int ackMode)
  {
    super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin, ackMode);
    if (ctx.sessionEntity != null)
    {
      publisherEntityList = (EntityList) sessionEntity.getEntity("publisher");
      subscriberEntityList = (EntityList) sessionEntity.getEntity("subscriber");
      durableEntityList = (EntityList) sessionEntity.getEntity("durable");
    }
  }

  public void visit(ProduceMessageRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitProduceMessageRequest");
    ctx.incMsgsSent(1);
    ProduceMessageReply reply = null;
    if (req.isReplyRequired())
      reply = (ProduceMessageReply) req.createReply();
    int producerId = req.getQueueProducerId();
    Producer producer = null;
    try
    {
      MessageImpl msg = SMQPUtil.getMessage(req);
      long ttl = msg.getJMSExpiration();
      if (ttl > 0)
        msg.setJMSExpiration(System.currentTimeMillis()+ttl);
      if (producerId == -1)
      {
        TopicImpl topic = (TopicImpl) msg.getJMSDestination();
        if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
          ctx.authSwiftlet.verifyTopicSenderSubscription(topic.getTopicName(), ctx.activeLogin.getLoginId());
        producer = new TopicProducer(ctx, topic);
      } else
      {
        producer = (Producer) producerList.get(producerId);
      }
      QueuePushTransaction transaction = (QueuePushTransaction) producer.createTransaction();
      transaction.putMessage(msg);
      transaction.commit();
      if (req.isReplyRequired())
      {
        reply.setDelay(transaction.getFlowControlDelay());
        reply.setOk(true);
      }
      if (producerId == -1)
        producer.close();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/produce messages failed: " + e.getMessage());
      if (req.isReplyRequired())
      {
        reply.setOk(false);
        reply.setException(e);
      }
    }
    if (req.isReplyRequired())
      reply.send();
  }

  public void visit(CreatePublisherRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreatePublisherRequest");
    CreatePublisherReply reply = (CreatePublisherReply) req.createReply();
    try
    {
      ctx.activeLogin.getResourceLimitGroup().incProducers();
    } catch (ResourceLimitException e)
    {
      reply.setOk(false);
      reply.setException(new JMSException(e.toString()));
      reply.send();
      return;
    }
    TopicImpl topic = req.getTopic();
    try
    {
      if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
        ctx.authSwiftlet.verifyTopicSenderSubscription(topic.getTopicName(), ctx.activeLogin.getLoginId());
      int producerId;
      TopicProducer producer;
      producerId = ArrayListTool.setFirstFreeOrExpand(producerList, null);
      producer = new TopicProducer(ctx, topic);
      producerList.set(producerId, producer);
      reply.setTopicPublisherId(producerId);
      reply.setOk(true);
      if (publisherEntityList != null)
      {
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
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
      ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
      reply.setOk(false);
      reply.setException(e);
      ctx.activeLogin.getResourceLimitGroup().decProducers();
    }
    reply.send();
  }

  public void visit(CloseProducerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseProducerRequest");
    ctx.activeLogin.getResourceLimitGroup().decProducers();
    TopicProducer producer = null;
    producer = (TopicProducer) producerList.get(req.getQueueProducerId());
    producerList.set(req.getQueueProducerId(), null);
    try
    {
      producer.close();
    } catch (Exception ignored)
    {
    }
    if (ctx.sessionEntity != null)
      publisherEntityList.removeDynamicEntity(producer);
    CloseProducerReply reply = (CloseProducerReply) req.createReply();
    reply.setOk(true);
    reply.send();
  }

  public void visit(CreateSubscriberRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateSubscriberRequest");
    CreateSubscriberReply reply = (CreateSubscriberReply) req.createReply();
    try
    {
      ctx.activeLogin.getResourceLimitGroup().incConsumers();
    } catch (ResourceLimitException e)
    {
      reply.setOk(false);
      reply.setException(new JMSException(e.toString()));
      reply.send();
      return;
    }
    TopicImpl topic = req.getTopic();
    String messageSelector = req.getMessageSelector();
    boolean noLocal = req.isNoLocal();
    try
    {
      Entity subEntity = null;
      if (subscriberEntityList != null)
        subEntity = subscriberEntityList.createEntity();

      int consumerId = 0;
      TopicConsumer consumer = null;
      if (topic.getType() == DestinationFactory.TYPE_TOPIC)
      {
        consumerId = ArrayListTool.setFirstFreeOrExpand(consumerList, null);
        consumer = new TopicConsumer(ctx, topic, messageSelector, noLocal);
        consumerList.set(consumerId, consumer);
        if (subEntity != null)
        {
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
      } else
      {
        consumerId = ArrayListTool.setFirstFreeOrExpand(consumerList, null);
        consumer = new TopicConsumer(ctx, topic, messageSelector, noLocal);
        consumerList.set(consumerId, consumer);
        if (subEntity != null)
          subEntity.setDynamicObject(consumer);
        if (subEntity != null)
        {
          subEntity.setName(topic.getQueueName() + "-" + consumerId);
          Property prop = subEntity.getProperty("temptopic");
          prop.setReadOnly(false);
          prop.setValue(new Boolean(true));
          prop.setReadOnly(true);
          prop = subEntity.getProperty("boundto");
          prop.setReadOnly(false);
          prop.setValue(topic.getQueueName());
          prop.setReadOnly(true);
        }
      }
      consumer.setAutoCommit(req.isAutoCommit());
      consumer.createReadTransaction();
      consumer.createTransaction();
      reply.setOk(true);
      reply.setTopicSubscriberId(consumerId);
      reply.setTmpQueueName((consumer).getQueueName());

      if (subEntity != null)
      {
        Property prop = subEntity.getProperty("nolocal");
        prop.setReadOnly(false);
        prop.setValue(new Boolean(noLocal));
        prop.setReadOnly(true);
        subEntity.createCommands();
        prop = subEntity.getProperty("selector");
        if (messageSelector != null)
        {
          prop.setValue(messageSelector);
        }
        prop.setReadOnly(true);
        subscriberEntityList.addEntity(subEntity);
      }
    } catch (InvalidSelectorException e)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateSubscriber has invalid Selector: " + e);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateSubscriber has invalid Selector: " + e);
      reply.setOk(false);
      reply.setException(e);
    } catch (Exception e1)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create subscriber: " + e1);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create subscriber: " + e1);
      reply.setOk(false);
      reply.setException(e1);
    }
    reply.send();
  }

  public void visit(CloseConsumerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest");
    CloseConsumerReply reply = (CloseConsumerReply) req.createReply();
    int qcId = req.getQueueConsumerId();
    Consumer consumer = null;
    consumer = (Consumer) consumerList.get(qcId);
    consumerList.set(qcId, null);
    try
    {
      consumer.close();
    } catch (Exception ignored)
    {
    }
    if (subscriberEntityList != null)
      subscriberEntityList.removeDynamicEntity(consumer);
    ctx.activeLogin.getResourceLimitGroup().decConsumers();
    reply.setOk(true);
    reply.send();
    if (req.getConsumerException() != null)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest, consumer closed due to client runtime exception: " + req.getConsumerException());
      ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest, consumer closed due to client runtime exception: " + req.getConsumerException());
    }
  }

  public void visit(CreateDurableRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateDurableRequest");
    CreateDurableReply reply = (CreateDurableReply) req.createReply();
    try
    {
      ctx.activeLogin.getResourceLimitGroup().incConsumers();
    } catch (ResourceLimitException e)
    {
      reply.setOk(false);
      reply.setException(new JMSException(e.toString()));
      reply.send();
      return;
    }
    TopicImpl topic = req.getTopic();
    String messageSelector = req.getMessageSelector();
    boolean noLocal = req.isNoLocal();
    String durableName = req.getDurableName();
    try
    {
      int consumerId = 0;
      TopicDurableConsumer consumer = null;
      consumerId = ArrayListTool.setFirstFreeOrExpand(consumerList, null);
      consumer = new TopicDurableConsumer(ctx, durableName, topic, messageSelector, noLocal);
      consumerList.set(consumerId, consumer);
      consumer.createReadTransaction();
      consumer.createTransaction();
      reply.setOk(true);
      reply.setTopicSubscriberId(consumerId);
      reply.setQueueName(consumer.getQueueName() + '@' + SwiftletManager.getInstance().getRouterName());

      if (durableEntityList != null)
      {
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
        prop.setValue(new Boolean(noLocal));
        prop.setReadOnly(true);
        prop = durEntity.getProperty("selector");
        if (messageSelector != null)
        {
          prop.setValue(messageSelector);
        }
        prop.setReadOnly(true);
        durableEntityList.addEntity(durEntity);
      }
    } catch (InvalidSelectorException e)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateDurable has invalid Selector: " + e);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateDurable has invalid Selector: " + e);
      reply.setOk(false);
      reply.setException(e);
    } catch (Exception e1)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create durable: " + e1);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create durable: " + e1);
      reply.setOk(false);
      reply.setException(e1);
    }
    reply.send();
  }

  // FIXIT: Check for existing consumer on it and throw exception appropriate
  public void visit(DeleteDurableRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitDeleteDurableRequest");
    DeleteDurableReply reply = (DeleteDurableReply) req.createReply();
    String durableName = req.getDurableName();
    try
    {
      ctx.topicManager.deleteDurable(durableName, ctx.activeLogin);
      reply.setOk(true);
      if (durableEntityList != null)
        durableEntityList.removeEntity(durableEntityList.getEntity(ctx.activeLogin.getClientId() + "$" + durableName));
    } catch (Exception e1)
    {
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during delete durable: " + e1);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during delete durable: " + e1);
      reply.setOk(false);
      reply.setException(e1);
    }
    reply.send();
  }

  public String toString()
  {
    return "NontransactedTopicSession, dispatchId=" + dispatchId;
  }
}

