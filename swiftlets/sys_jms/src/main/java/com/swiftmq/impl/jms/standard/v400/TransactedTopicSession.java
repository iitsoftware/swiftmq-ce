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

package com.swiftmq.impl.jms.standard.v400;

import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.smqp.v400.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.tools.collection.*;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;

public class TransactedTopicSession extends TransactedSession
{
  EntityList publisherEntityList = null;
  EntityList subscriberEntityList = null;
  EntityList durableEntityList = null;

  public TransactedTopicSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin)
  {
    super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin);
    if (ctx.sessionEntity != null)
    {
      publisherEntityList = (EntityList) sessionEntity.getEntity("publisher");
      subscriberEntityList = (EntityList) sessionEntity.getEntity("subscriber");
      durableEntityList = (EntityList) sessionEntity.getEntity("durable");
    }
  }

  public void visitCommitRequest(CommitRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCommitRequest");
    CommitReply reply = (CommitReply) req.createReply();
    reply.setOk(true);
    try
    {
      // first: produce all messages
      Object[] wrapper = req.getMessages();
      ctx.incMsgsSent(wrapper.length);
      req.setMessages(null);
      long fcDelay = 0;
      RingBuffer tempProducers = null;
      for (int i = 0; i < wrapper.length; i++)
      {
        DataByteArrayInputStream dis = new DataByteArrayInputStream((byte[]) wrapper[i]);
        int producerId = dis.readInt();
        int type = dis.readInt();
        MessageImpl msg = MessageImpl.createInstance(type);
        msg.readContent(dis);
        dis.close();
        Producer producer = null;
        if (producerId == -1)
        {
          TopicImpl topic = (TopicImpl) msg.getJMSDestination();
          if (topic.getType() != DestinationFactory.TYPE_TEMPTOPIC)
            ctx.authSwiftlet.verifyTopicSenderSubscription(topic.getTopicName(), ctx.activeLogin.getLoginId());
          producer = new TopicProducer(ctx, topic);
          if (tempProducers == null)
            tempProducers = new RingBuffer(8);
          tempProducers.add(producer);
          transactionManager.addTransactionFactory(producer);
        } else
        {
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
      if (tempProducers != null)
      {
        int size = tempProducers.getSize();
        for (int i = 0; i < size; i++)
        {
          ((Producer) tempProducers.remove()).close();
        }
      }
      reply.setDelay(fcDelay);
      purgeMarkedProducers();
      purgeMarkedConsumers();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/commit produced messages failed: " + e.getMessage());
      reply.setOk(false);
      reply.setException((e instanceof JMSException)?e:new javax.jms.JMSException(e.toString()));
    }
    reply.send();
  }

  public void visitCreatePublisherRequest(CreatePublisherRequest req)
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

      // enlist it at the transaction manager
      transactionManager.addTransactionFactory(producer);
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
      ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating publisher: " + e.getMessage());
      reply.setOk(false);
      reply.setException((e instanceof JMSException)?e:new javax.jms.JMSException(e.toString()));
      ctx.activeLogin.getResourceLimitGroup().decProducers();
    }
    reply.send();
  }

  public void visitCloseProducerRequest(CloseProducerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseProducerRequest");
    ctx.activeLogin.getResourceLimitGroup().decProducers();
    TopicProducer producer = null;
    producer = (TopicProducer) producerList.get(req.getQueueProducerId());

    // mark for close and close it later after commit/rollback
    producer.markForClose();

    if (ctx.sessionEntity != null)
      publisherEntityList.removeDynamicEntity(producer);
    CloseProducerReply reply = (CloseProducerReply) req.createReply();
    reply.setOk(true);
    reply.send();
  }

  public void visitCreateSubscriberRequest(CreateSubscriberRequest req)
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
      reply.setOk(true);
      reply.setTopicSubscriberId(consumerId);

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

      consumer.createReadTransaction();
      // enlist it at the transaction manager
      transactionManager.addTransactionFactory(consumer);
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
      reply.setException(new javax.jms.JMSException(e1.toString()));
    }
    reply.send();
  }

  public void visitCloseConsumerRequest(CloseConsumerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseConsumerRequest");
    CloseConsumerReply reply = (CloseConsumerReply) req.createReply();
    int qcId = req.getQueueConsumerId();
    Consumer consumer = (Consumer) consumerList.get(qcId);

    // mark for close and close it later after commit/rollback
    consumer.markForClose();

    if (subscriberEntityList != null)
      subscriberEntityList.removeDynamicEntity(consumer);
    ctx.activeLogin.getResourceLimitGroup().decConsumers();
    reply.setOk(true);
    reply.send();
  }

  public void visitCreateDurableRequest(CreateDurableRequest req)
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
      reply.setOk(true);
      reply.setTopicSubscriberId(consumerId);

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

      consumer.createReadTransaction();
      // enlist it at the transaction manager
      transactionManager.addTransactionFactory(consumer);
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
      reply.setException((e1 instanceof JMSException)?e1:new javax.jms.JMSException(e1.toString()));
    }
    reply.send();
  }

  // FIXIT: Check for existing consumer on it and throw exception appropriate
  public void visitDeleteDurableRequest(DeleteDurableRequest req)
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
      reply.setException(new javax.jms.JMSException(e1.toString()));
    }
    reply.send();
  }

  protected void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
    super.close();
    transactionManager.close();
  }

  public String toString()
  {
    return "TransactedTopicSession, dispatchId=" + dispatchId;
  }
}

