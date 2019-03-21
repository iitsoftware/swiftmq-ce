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

package com.swiftmq.impl.jms.standard.v510;

import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.v510.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.*;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.queue.SingleProcessorQueue;

import javax.jms.*;

public class NontransactedQueueSession extends NontransactedSession
{
  BrowserManager browserManager = null;
  EntityList senderEntityList = null;
  EntityList receiverEntityList = null;

  public NontransactedQueueSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin, int ackMode)
  {
    super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin, ackMode);
    browserManager = new BrowserManager(ctx);
    if (ctx.sessionEntity != null)
    {
      senderEntityList = (EntityList) sessionEntity.getEntity("sender");
      receiverEntityList = (EntityList) sessionEntity.getEntity("receiver");
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
      if (producerId == -1)
      {
        String queueName = ((QueueImpl) msg.getJMSDestination()).getQueueName();
        if (!ctx.queueManager.isQueueRunning(queueName))
          throw new InvalidDestinationException("Invalid destination: " + queueName);
        producer = new QueueProducer(ctx, queueName);
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

  public void visit(CreateProducerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateProducerRequest");
    CreateProducerReply reply = (CreateProducerReply) req.createReply();
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
    QueueImpl queue = req.getQueue();
    try
    {
      int producerId;
      QueueProducer producer;
      producerId = ArrayListTool.setFirstFreeOrExpand(producerList, null);
      producer = new QueueProducer(ctx, queue.getQueueName());
      producerList.set(producerId, producer);
      reply.setQueueProducerId(producerId);
      reply.setOk(true);
      if (senderEntityList != null)
      {
        Entity senderEntity = senderEntityList.createEntity();
        senderEntity.setName(queue.getQueueName() + "-" + producerId);
        senderEntity.setDynamicObject(producer);
        senderEntity.createCommands();
        Property prop = senderEntity.getProperty("queue");
        prop.setValue(queue.getQueueName());
        prop.setReadOnly(true);
        senderEntityList.addEntity(senderEntity);
      }
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
      ctx.logSwiftlet.logError("sys$jms", ctx.tracePrefix + "/exception creating producer: " + e.getMessage());
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
    QueueProducer producer = null;
    producer = (QueueProducer) producerList.get(req.getQueueProducerId());
    producerList.set(req.getQueueProducerId(), null);
    try
    {
      producer.close();
    } catch (Exception ignored)
    {
    }
    if (ctx.sessionEntity != null)
      senderEntityList.removeDynamicEntity(producer);
    CloseProducerReply reply = (CloseProducerReply) req.createReply();
    reply.setOk(true);
    reply.send();
  }

  public void visit(CreateConsumerRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateConsumerRequest");
    CreateConsumerReply reply = (CreateConsumerReply) req.createReply();
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
    QueueImpl queue = req.getQueue();
    String messageSelector = req.getMessageSelector();
    String queueName = null;
    try
    {
      queueName = queue.getQueueName();
    } catch (JMSException ignored)
    {
    }
    try
    {
      if (!queueName.endsWith('@' + SwiftletManager.getInstance().getRouterName()))
        throw new InvalidDestinationException("Queue '" + queueName + "' is not local! Can't create a Consumer on it!");
      int consumerId = 0;
      QueueConsumer consumer = null;
      consumerId = ArrayListTool.setFirstFreeOrExpand(consumerList, null);
      consumer = new QueueConsumer(ctx, queueName, messageSelector);
      consumerList.set(consumerId, consumer);
      consumer.createReadTransaction();
      consumer.createTransaction();
      reply.setOk(true);
      reply.setQueueConsumerId(consumerId);
      if (receiverEntityList != null)
      {
        Entity consEntity = receiverEntityList.createEntity();
        consEntity.setName(queueName + "-" + consumerId);
        consEntity.setDynamicObject(consumer);
        consEntity.createCommands();
        Property prop = consEntity.getProperty("queue");
        prop.setValue(queueName);
        prop.setReadOnly(true);
        prop = consEntity.getProperty("selector");
        if (messageSelector != null)
        {
          prop.setValue(messageSelector);
        }
        prop.setReadOnly(true);
        receiverEntityList.addEntity(consEntity);
      }
    } catch (InvalidSelectorException e)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/CreateConsumer has invalid Selector: " + e);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/CreateConsumer has invalid Selector: " + e);
      reply.setOk(false);
      reply.setException(e);
    } catch (Exception e1)
    {
      ctx.activeLogin.getResourceLimitGroup().decConsumers();
      ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/Exception during create consumer: " + e1);
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/Exception during create consumer: " + e1);
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
    QueueConsumer consumer = null;
    consumer = (QueueConsumer) consumerList.get(qcId);
    consumerList.set(qcId, null);
    try
    {
      consumer.close();
    } catch (Exception ignored)
    {
    }
    if (receiverEntityList != null)
      receiverEntityList.removeDynamicEntity(consumer);
    ctx.activeLogin.getResourceLimitGroup().decConsumers();
    reply.setOk(true);
    reply.send();
  }

  public void visit(CreateBrowserRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCreateBrowserRequest");
    browserManager.createBrowser(req);
  }

  public void visit(FetchBrowserMessageRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
    browserManager.fetchBrowserMessage(req);
  }

  public void visit(CloseBrowserRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitFetchBrowserMessageRequest");
    browserManager.closeBrowser(req);
  }

  protected void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
    super.close();
    browserManager.close();
  }

  public String toString()
  {
    return "NontransactedQueueSession, dispatchId=" + dispatchId;
  }
}

