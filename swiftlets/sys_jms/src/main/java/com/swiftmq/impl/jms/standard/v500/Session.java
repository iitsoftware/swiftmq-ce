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

import com.swiftmq.jms.smqp.v500.CloseSessionRequest;
import com.swiftmq.jms.smqp.v500.MessageDeliveredRequest;
import com.swiftmq.jms.smqp.v500.StartConsumerRequest;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;

import java.util.ArrayList;

public abstract class Session extends SessionVisitor
    implements RequestService
{
  static final String TP_SESSIONSVC = "sys$jms.session.service";

  protected ArrayList consumerList = new ArrayList();
  protected ArrayList producerList = new ArrayList();
  protected SessionContext ctx = null;
  protected int dispatchId;
  protected ThreadPool sessionTP = null;
  protected int recoveryEpoche = 0;
  protected boolean recoveryInProgress = false;
  protected boolean closed = false;

  public Session(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin)
  {
    this.dispatchId = dispatchId;
    ctx = new SessionContext();
    ctx.queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    ctx.topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
    ctx.authSwiftlet = (AuthenticationSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$authentication");
    ctx.threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    ctx.logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    ctx.traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    ctx.traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    ctx.tracePrefix = connectionTracePrefix + "/" + toString();
    ctx.activeLogin = activeLogin;
    ctx.sessionEntity = sessionEntity;
    sessionTP = ctx.threadpoolSwiftlet.getPool(TP_SESSIONSVC);
    ctx.sessionQueue = new SessionQueue(sessionTP, this);
    ctx.connectionOutboundQueue = connectionOutboundQueue;
    ctx.sessionQueue.startQueue();
  }

  protected Session(String connectionTracePrefix, Entity sessionEntity, SingleProcessorQueue connectionOutboundQueue, int dispatchId, ActiveLogin activeLogin, int ackMode)
  {
    this(connectionTracePrefix, sessionEntity, connectionOutboundQueue, dispatchId, activeLogin);
    ctx.ackMode = ackMode;
  }

  public void visitStartConsumerRequest(StartConsumerRequest req)
  {
    if (closed)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitStartConsumerRequest");
    int qcId = req.getQueueConsumerId();
    Consumer consumer = (Consumer) consumerList.get(qcId);
    if (consumer == null)
      return;
    int clientDispatchId = req.getClientDispatchId();
    int clientListenerId = req.getClientListenerId();
    try
    {
      MessageProcessor mp = consumer.getMessageProcessor();
      if (mp == null)
      {
        mp = new AsyncMessageProcessor(this, ctx, consumer, req.getConsumerCacheSize(), recoveryEpoche);
        consumer.setMessageListener(clientDispatchId, clientListenerId, mp);
      }
      QueuePullTransaction t = consumer.getReadTransaction();
      if (t != null && !t.isClosed())
        t.registerMessageProcessor(mp);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void visitDeliveryItem(DeliveryItem item)
  {
    if (closed || recoveryInProgress || item.request.getRecoveryEpoche() != recoveryEpoche)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitDeliveryItem, item= " + item);
    QueuePullTransaction rt = (QueuePullTransaction) item.consumer.getReadTransaction();
    QueuePullTransaction t = (QueuePullTransaction) item.consumer.getTransaction();
    try
    {
      item.request.setMessageEntry(item.messageEntry);
      ctx.connectionOutboundQueue.enqueue(item.request);
    } catch (Exception e)
    {
      if (!closed)
      {
        e.printStackTrace();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/handleDelivery, exception= " + e);
      }
    }
  }

  public void visitMessageDeliveredRequest(MessageDeliveredRequest req)
  {
    if (closed || recoveryInProgress)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest");
    try
    {
      Consumer consumer = (Consumer) consumerList.get(req.getQueueConsumerId());
      QueuePullTransaction rt = (QueuePullTransaction) consumer.getReadTransaction();
      QueuePullTransaction t = (QueuePullTransaction) consumer.getTransaction();
      t.moveToTransaction(req.getMessageIndex(), rt);
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitMessageDeliveredRequest, exception="+e);
      e.printStackTrace();
    }
  }

  public void visitCloseSessionRequest(CloseSessionRequest request)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseSessionRequest...");
    close();
    request.sem.notifySingleWaiter();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/visitCloseSessionRequest...DONE");
  }

  public void serviceRequest(Request request)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/serviceRequest, request=" + request);
    ctx.sessionQueue.enqueue(request);
  }

  protected void close()
  {
    closed = true;
    ctx.sessionQueue.stopQueue();

    for (int i = 0; i < consumerList.size(); i++)
    {
      Consumer consumer = (Consumer) consumerList.get(i);
      if (consumer != null)
      {
        try
        {
          consumer.close();
        } catch (Exception e)
        {
        }
        ctx.activeLogin.getResourceLimitGroup().decConsumers();
      }
    }
    for (int i = 0; i < producerList.size(); i++)
    {
      Producer producer = (Producer) producerList.get(i);
      if (producer != null)
      {
        try
        {
          producer.close();
        } catch (Exception e)
        {
        }
      }
      ctx.activeLogin.getResourceLimitGroup().decProducers();
    }
  }

  protected boolean isClosed()
  {
    return closed;
  }

  public String toString()
  {
    return "Session, dispatchId=" + dispatchId;
  }

}

