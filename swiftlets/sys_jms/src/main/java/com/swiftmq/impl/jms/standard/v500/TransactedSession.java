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

import com.swiftmq.jms.smqp.v500.RollbackReply;
import com.swiftmq.jms.smqp.v500.RollbackRequest;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.GenericRequest;

public abstract class TransactedSession extends Session
{
  TransactionManager transactionManager;
  DeliveryItem currentItem = null;

  public TransactedSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin)
  {
    super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin);
    transactionManager = new TransactionManager(ctx);
    ctx.transacted = true;
  }

  protected void purgeMarkedProducers() throws Exception
  {
    for (int i = 0; i < producerList.size(); i++)
    {
      Producer producer = (Producer) producerList.get(i);
      if (producer != null && producer.isMarkedForClose())
      {
        producer.close();
        producerList.set(i, null);
      }
    }
  }

  protected void purgeMarkedConsumers() throws Exception
  {
    for (int i = 0; i < consumerList.size(); i++)
    {
      Consumer consumer = (Consumer) consumerList.get(i);
      if (consumer != null && consumer.isMarkedForClose())
      {
        consumer.close();
        consumerList.set(i, null);
      }
    }
  }

  public void visitGenericRequest(GenericRequest request)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitGenericRequest/RollbackReply");
    RollbackReply reply = (RollbackReply) request.getPayload();
    recoveryEpoche++;
    try
    {
      transactionManager.rollback(false);
      purgeMarkedProducers();
      purgeMarkedConsumers();
      for (int i = 0; i < consumerList.size(); i++)
      {
        Consumer consumer = (Consumer) consumerList.get(i);
        if (consumer != null)
        {
          consumer.createReadTransaction();
          AsyncMessageProcessor mp = (AsyncMessageProcessor) consumer.getMessageProcessor();
          if (mp != null)
          {
            mp = new AsyncMessageProcessor(this, ctx, consumer, mp.getConsumerCacheSize(), recoveryEpoche);
            consumer.setMessageListener(consumer.getClientDispatchId(), consumer.getClientListenerId(), mp);
            consumer.getReadTransaction().registerMessageProcessor(mp);
          }
        }
      }
      transactionManager.startTransactions();
    } catch (Exception e)
    {
      reply.setOk(false);
      reply.setException(new javax.jms.JMSException(e.toString()));
    }
    recoveryInProgress = false;
    reply.send();
  }

  public void visitRollbackRequest(RollbackRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRollbackRequest");
    recoveryInProgress = true;
    RollbackReply reply = (RollbackReply) req.createReply();
    reply.setOk(true);
    for (int i = 0; i < consumerList.size(); i++)
    {
      Consumer consumer = (Consumer) consumerList.get(i);
      if (consumer != null)
      {
        try
        {
          MessageProcessor mp = consumer.getMessageProcessor();
          if (mp != null)
          {
            mp.stop();
            consumer.getReadTransaction().unregisterMessageProcessor(mp);
          }
          consumer.getReadTransaction().rollback();
        } catch (Exception e)
        {
          reply.setOk(false);
          reply.setException(new javax.jms.JMSException(e.toString()));
          break;
        }
      }
    }

    GenericRequest gr = new GenericRequest(-1, false, reply);
    ctx.sessionQueue.enqueue(gr);
  }

  public String toString()
  {
    return "TransactedSession, dispatchId=" + dispatchId;
  }
}

