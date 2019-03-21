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

package com.swiftmq.impl.jms.standard.v610;

import com.swiftmq.jms.smqp.v610.AcknowledgeMessageReply;
import com.swiftmq.jms.smqp.v610.AcknowledgeMessageRequest;
import com.swiftmq.jms.smqp.v610.MessageDeliveredRequest;
import com.swiftmq.jms.smqp.v610.RecoverSessionReply;
import com.swiftmq.jms.smqp.v610.RecoverSessionRequest;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.GenericRequest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NontransactedSession extends Session
{
  protected List deliveredList = null;

  public NontransactedSession(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin, int ackMode)
  {
    super(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin, ackMode);
    ctx.transacted = false;
    deliveredList = new ArrayList();
  }

  public void visit(MessageDeliveredRequest req)
  {
    if (closed || recoveryInProgress)
      return;
    if (!req.isDuplicate())
      deliveredList.add(req);
    super.visit(req);
  }

  public void visit(AcknowledgeMessageRequest req)
  {
    if (closed)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitAcknowledgeMessageRequest");
    AcknowledgeMessageReply reply = null;
    if (req.isReplyRequired())
    {
      reply = (AcknowledgeMessageReply) req.createReply();
      reply.setOk(true);
    }
    try
    {
      MessageIndex ackIndex = req.getMessageIndex();
      if (ctx.ackMode == javax.jms.Session.CLIENT_ACKNOWLEDGE)
      {
        boolean found = false;
        Iterator iter = deliveredList.iterator();
        while (iter.hasNext())
        {
          MessageDeliveredRequest request = (MessageDeliveredRequest) iter.next();
          Consumer consumer = (Consumer) consumerList.get(request.getQueueConsumerId());
          MessageIndex actIndex = request.getMessageIndex();
          QueuePullTransaction t = consumer.getTransaction();
          try
          {
            if (!t.isClosed())
              t.acknowledgeMessage(actIndex);
          } catch (Exception e)
          {
            // temp queue might be deleted in the mean time
          }
          iter.remove();
        }
      } else
      {
        Consumer consumer = (Consumer) consumerList.get(req.getQueueConsumerId());
        QueuePullTransaction t = consumer.getReadTransaction();
        try
        {
          if (!t.isClosed())
            t.acknowledgeMessage(ackIndex);
        } catch (Exception e)
        {
          // temp queue might be deleted in the mean time
        }
      }
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitAcknowledgeMessageRequest, exception: " + e);
      if (reply != null)
      {
        reply.setOk(false);
        reply.setException(e);
      }
    }

    if (reply != null)
      reply.send();
  }

  public void visitGenericRequest(GenericRequest request)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitGenericRequest/RecoverSessionReply");
    RecoverSessionReply reply = (RecoverSessionReply) request.getPayload();
    recoveryEpoche = reply.getRecoveryEpoche();
    try
    {
      for (int i = 0; i < consumerList.size(); i++)
      {
        Consumer consumer = (Consumer) consumerList.get(i);
        if (consumer != null)
        {
          consumer.createReadTransaction();
          consumer.createTransaction();
          AsyncMessageProcessor mp = (AsyncMessageProcessor) consumer.getMessageProcessor();
          if (mp != null)
          {
            mp = new AsyncMessageProcessor(this, ctx, consumer, mp.getConsumerCacheSize(), recoveryEpoche);
            consumer.setMessageListener(consumer.getClientDispatchId(), consumer.getClientListenerId(), mp);
            consumer.getReadTransaction().registerMessageProcessor(mp);
          }
        }
      }
    } catch (Exception e)
    {
      reply.setOk(false);
      reply.setException(e);
    }
    recoveryInProgress = false;
    reply.send();
  }

  public void visit(RecoverSessionRequest req)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitRecoverSessionRequest");
    recoveryInProgress = true;
    RecoverSessionReply reply = (RecoverSessionReply) req.createReply();
    reply.setRecoveryEpoche(req.getRecoveryEpoche());
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
          consumer.getTransaction().rollback();
        } catch (Exception e)
        {
          reply.setOk(false);
          reply.setException(e);
          break;
        }
      }
    }
    deliveredList.clear();
    GenericRequest gr = new GenericRequest(-1, false, reply);
    ctx.sessionQueue.enqueue(gr);
  }

  protected void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/close");
    super.close();
    deliveredList.clear();
  }

  public String toString()
  {
    return "NontransactedSession, dispatchId=" + dispatchId;
  }
}

