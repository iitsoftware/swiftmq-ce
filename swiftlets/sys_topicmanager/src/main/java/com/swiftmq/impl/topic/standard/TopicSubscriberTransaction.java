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

package com.swiftmq.impl.topic.standard;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

public class TopicSubscriberTransaction
{
  TopicSubscription topicSubscription;
  QueuePushTransaction transaction;
  String destination = null;
  boolean valid = true;
  boolean persistentMessageIncluded = false;
  volatile long fcDelay = 0;

  /**
   * @param topicSubscription
   * @param transaction
   * @SBGen Constructor assigns topicSubscription, transaction
   */
  protected TopicSubscriberTransaction(TopicSubscription topicSubscription, QueuePushTransaction transaction, String destination)
  {
    // SBgen: Assign variables
    this.topicSubscription = topicSubscription;
    this.transaction = transaction;
    this.destination = destination;
    // SBgen: End assign
  }

  public long getFcDelay()
  {
    return fcDelay;
  }

  public boolean isPersistentMessageIncluded()
  {
    return persistentMessageIncluded;
  }

  public void setPersistentMessageIncluded(boolean persistentMessageIncluded)
  {
    this.persistentMessageIncluded = persistentMessageIncluded;
  }

  protected TopicSubscription getTopicSubscription()
  {
    return topicSubscription;
  }

  public QueuePushTransaction getTransaction()
  {
    return transaction;
  }

  protected void publish(MessageImpl message)
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    transaction.putMessage(message);
  }

  protected void prepare(XidImpl globalTxId)
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    try
    {
      transaction.prepare(globalTxId);
    } catch (QueueException e)
    {
      if (transaction.isQueueRunning())
        throw e;
    }
  }

  protected long commit(XidImpl globalTxId)
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    try
    {
      transaction.commit(globalTxId);
    } catch (QueueException e)
    {
      if (transaction.isQueueRunning())
        throw e;
    }
    topicSubscription.removeTransaction(this);
    valid = false;
    return transaction.getFlowControlDelay();
  }

  protected long commit()
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    transaction.commit();
    topicSubscription.removeTransaction(this);
    valid = false;
    return transaction.getFlowControlDelay();
  }

  protected void commit(AsyncCompletionCallback callback)
  {
    if (!valid)
    {
      callback.setException(new QueueTransactionClosedException("TopicSubscriberTransaction is invalid"));
      callback.done(false);
      return;
    }
    transaction.commit(new AsyncCompletionCallback(callback)
    {
      public void done(boolean success)
      {
        topicSubscription.removeTransaction(TopicSubscriberTransaction.this);
        valid = false;
        fcDelay = transaction.getFlowControlDelay();
        if (success)
          next.setResult(Long.valueOf(fcDelay));
        else
          next.setException(getException());
      }
    });
  }

  protected void rollback(XidImpl globalTxId)
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    transaction.rollback(globalTxId, false);
    topicSubscription.removeTransaction(this);
    valid = false;
  }

  protected void rollback()
      throws Exception
  {
    if (!valid)
      throw new QueueTransactionClosedException("TopicSubscriberTransaction is invalid");
    transaction.rollback();
    topicSubscription.removeTransaction(this);
    valid = false;
  }

  protected void rollback(AsyncCompletionCallback callback)
  {
    if (!valid)
    {
      callback.setException(new QueueTransactionClosedException("TopicSubscriberTransaction is invalid"));
      callback.done(false);
      return;
    }
    transaction.rollback(new AsyncCompletionCallback(callback)
    {
      public void done(boolean success)
      {
        topicSubscription.removeTransaction(TopicSubscriberTransaction.this);
        valid = false;
        if (!success)
          next.setException(getException());
      }
    });
  }

  /**
   * @return
   * @SBGen Method get valid
   */
  protected boolean isValid()
  {
    // SBgen: Get variable
    return (valid);
  }

  public String toString()
  {
    return "[TopicSubscriberTransaction, sub=" + topicSubscription + "]";
  }
}

