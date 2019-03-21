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

import com.swiftmq.jms.smqp.v400.AsyncMessageDeliveryRequest;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

public class AsyncMessageProcessor extends MessageProcessor
{
  Session session = null;
  SessionContext ctx = null;
  Consumer consumer = null;
  int consumerCacheSize = 0;
  int recoveryEpoche = 0;
  int deliveryCount = 0;
  boolean valid = true;
  int numberMessages = 0;

  public AsyncMessageProcessor(Session session, SessionContext ctx, Consumer consumer, int consumerCacheSize, int recoveryEpoche)
  {
    super(consumer.getSelector());
    this.session = session;
    this.ctx = ctx;
    this.consumer = consumer;
    this.consumerCacheSize = consumerCacheSize;
    this.recoveryEpoche = recoveryEpoche;
    setAutoCommit(consumer.isAutoCommit());
    setBulkMode(true);
    createBulkBuffer(consumerCacheSize);
  }

  public int getConsumerCacheSize()
  {
    return consumerCacheSize;
  }

  public void setConsumerCacheSize(int consumerCacheSize)
  {
    this.consumerCacheSize = consumerCacheSize;
  }

  public boolean isValid()
  {
    return valid && !session.closed;
  }

  public void stop()
  {
    valid = false;
  }

  public void reset()
  {
    deliveryCount = 0;
    valid = true;
  }

  public void processMessages(int numberMessages)
  {
    this.numberMessages = numberMessages;
    if (isValid())
      session.sessionTP.dispatchTask(this);
  }

  public void processMessage(MessageEntry messageEntry)
  {
    throw new RuntimeException("Invalid method call, bulk mode is enabled!");
  }

  public void processException(Exception exception)
  {
    valid = !(exception instanceof QueueHandlerClosedException);
  }

  public void run()
  {
    if (!isValid())
      return;
    ctx.incMsgsReceived(numberMessages);
    deliveryCount += numberMessages;
    boolean restart = deliveryCount >= consumerCacheSize;
    MessageEntry[] buffer = getBulkBuffer();
    if (isAutoCommit())
    {
      AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), buffer, numberMessages, session.dispatchId);
      request.setRequiresRestart(restart);
      request.setRecoveryEpoche(recoveryEpoche);
      ctx.connectionOutboundQueue.enqueue(request);
    } else
    {
      for (int i = 0; i < numberMessages; i++)
      {
        AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), buffer[i], session.dispatchId);
        request.setRequiresRestart(i == numberMessages - 1 && restart);
        request.setRecoveryEpoche(recoveryEpoche);

        DeliveryItem item = new DeliveryItem();
        item.messageEntry = buffer[i];
        item.consumer = consumer;
        item.request = request;
        ctx.sessionQueue.enqueue(item);
      }
    }
    try
    {
      if (!restart)
      {
        QueuePullTransaction t = consumer.getReadTransaction();
        if (t != null && !t.isClosed())
        {
          try
          {
            t.registerMessageProcessor(this);
          } catch (QueueTransactionClosedException e)
          {
          }
        }
      } else
        deliveryCount = 0;
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public String getDescription()
  {
    return session.toString() + "/AsyncMessageProcessor";
  }

  public String getDispatchToken()
  {
    return session.TP_SESSIONSVC;
  }
}
