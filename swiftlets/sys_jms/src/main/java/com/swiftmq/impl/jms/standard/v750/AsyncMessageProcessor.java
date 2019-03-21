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

import com.swiftmq.impl.jms.standard.accounting.DestinationCollector;
import com.swiftmq.jms.smqp.v750.AsyncMessageDeliveryRequest;
import com.swiftmq.swiftlet.queue.*;

public class AsyncMessageProcessor extends MessageProcessor
{
  RegisterMessageProcessor registerRequest = null;
  RunMessageProcessor runRequest = null;
  Session session = null;
  SessionContext ctx = null;
  Consumer consumer = null;
  int consumerCacheSize = 0;
  int recoveryEpoche = 0;
  int deliveryCount = 0;
  boolean valid = true;
  int numberMessages = 0;
  int lowWaterMark = 0;
  long maxBulkSize = -1;
  volatile boolean started = false;

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
    registerRequest = new RegisterMessageProcessor(this);
    runRequest = new RunMessageProcessor(this);
    lowWaterMark = session.getMyConnection().ctx.consumerCacheLowWaterMark;
    if (lowWaterMark * 2 >= consumerCacheSize)
      lowWaterMark = 0;
  }

  public void setMaxBulkSize(long maxBulkSize)
  {
    this.maxBulkSize = maxBulkSize == -1 ? maxBulkSize : maxBulkSize * 1024;
  }

  public long getMaxBulkSize()
  {
    return maxBulkSize;
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
    {
      ctx.sessionQueue.enqueue(runRequest);
    }
  }

  public void processMessage(MessageEntry messageEntry)
  {
    throw new RuntimeException("Invalid method call, bulk mode is enabled!");
  }

  public void processException(Exception exception)
  {
    valid = !(exception instanceof QueueHandlerClosedException);
  }

  public boolean isStarted()
  {
    return started;
  }

  public void register()
  {
    if (!isValid())
      return;
    try
    {
      QueuePullTransaction t = consumer.getReadTransaction();
      if (t != null && !t.isClosed())
      {
        try
        {
          started = true;
          //         t.unregisterMessageProcessor(this);
          t.registerMessageProcessor(this);
        } catch (QueueTransactionClosedException e)
        {
        }
      }
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void run()
  {
    if (!isValid())
      return;
    ctx.incMsgsReceived(numberMessages);
    deliveryCount += numberMessages;
    boolean restart = false;
    if (maxBulkSize != -1)
    {
      maxBulkSize -= getCurrentBulkSize();
      restart = deliveryCount >= consumerCacheSize - lowWaterMark || maxBulkSize <= 0;
    } else
      restart = deliveryCount >= consumerCacheSize - lowWaterMark;
    MessageEntry[] buffer = getBulkBuffer();
    if (isAutoCommit())
    {
      DestinationCollector collector = consumer.getCollector();
      if (collector != null)
        collector.incTotal(numberMessages, getCurrentBulkSize());
      MessageEntry[] bulk = new MessageEntry[numberMessages];
      System.arraycopy(buffer, 0, bulk, 0, numberMessages);
      AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), null, bulk, session.dispatchId, restart, recoveryEpoche);
      ctx.connectionOutboundQueue.enqueue(request);
    } else
    {
      for (int i = 0; i < numberMessages; i++)
      {
        AsyncMessageDeliveryRequest request = new AsyncMessageDeliveryRequest(consumer.getClientDispatchId(), consumer.getClientListenerId(), buffer[i], null, session.dispatchId, i == numberMessages - 1 && restart, recoveryEpoche);
        DeliveryItem item = new DeliveryItem();
        item.messageEntry = buffer[i];
        item.consumer = consumer;
        item.request = request;
        ctx.sessionQueue.enqueue(item);
      }
    }
    if (!restart)
    {
      ctx.sessionQueue.enqueue(registerRequest);
    } else
    {
      deliveryCount = 0;
      maxBulkSize = -1;
      started = false;
    }
  }

  public String getDescription()
  {
    return session.toString() + "/AsyncMessageProcessor";
  }

  public String getDispatchToken()
  {
    return Session.TP_SESSIONSVC;
  }
}
