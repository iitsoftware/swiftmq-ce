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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

public class ClusteredQueue extends AbstractQueue
{
  SwiftletContext ctx = null;
  DispatchPolicy dispatchPolicy = null;
  CompositeStoreTransaction compositeTx = null;

  public ClusteredQueue(SwiftletContext ctx, DispatchPolicy dispatchPolicy)
  {
    this.ctx = ctx;
    this.dispatchPolicy = dispatchPolicy;
    if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), toString() + "/created");
  }

  public AbstractQueue selectBaseQueue()
  {
    String queueName = dispatchPolicy.getNextReceiveQueue();
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/selectBaseQueue, nextReceiveQueue=" + queueName);
    if (queueName == null)
      return super.selectBaseQueue();
    return ctx.queueManager.getQueueForInternalUse(queueName);
  }

  public boolean hasReceiver(MessageImpl message)
  {
    return false;
  }

  public void lockQueue(Object txId)
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/lockQueue, cTxId=" + cTxId);
    cTxId.lockQueue();
  }

  public void unlockQueue(Object txId, boolean markAsyncActive)
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/unlockQueue, cTxId=" + cTxId);
    cTxId.unlockQueue(markAsyncActive);
  }

  public void unmarkAsyncActive(Object txId)
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/unmarkAsyncActive, cTxId=" + cTxId);
    cTxId.unmarkAsyncActive();
  }

  public void setCompositeStoreTransaction(Object txId, CompositeStoreTransaction ct)
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/setCompositeStoreTransaction, cTxId=" + cTxId);
    cTxId.setCompositeStoreTransaction(ct);
  }

  public CompositeStoreTransaction getCompositeStoreTransaction(Object txId)
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    return cTxId.getCompositeStoreTransaction();
  }

  public Object createPullTransaction() throws QueueException
  {
    throw new QueueException("Operation not supported!");
  }

  public Object createPushTransaction() throws QueueException
  {
    ClusteredTransactionId txId = null;
    if (dispatchPolicy.isMessageBasedDispatch())
    {
      txId = new ClusteredTransactionId(ctx, true);
    } else
    {
      String queueName = dispatchPolicy.getNextSendQueue();
      if (ctx.queueSpace.enabled)
        ctx.queueSpace.trace(getQueueName(), toString() + "/createPushTransaction, nextSendQueue=" + queueName);
      if (queueName == null)
        throw new QueueException("Unable to select a physical destination queue for this clustered queue!");
      AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(queueName, true);
      txId = new ClusteredTransactionId(ctx, queue, queue.createPushTransaction(), new QueueImpl(queueName));
    }
    return txId;
  }

  public void putMessage(Object txId, MessageImpl message) throws QueueException
  {
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    if (cTxId.isMessageBasedDispatch())
    {
      if (ctx.queueSpace.enabled)
        ctx.queueSpace.trace(getQueueName(), toString() + "/putMessage, message based dispatch ...");
      String queueName = dispatchPolicy.getNextSendQueue(message);
      if (ctx.queueSpace.enabled)
        ctx.queueSpace.trace(getQueueName(), toString() + "/putMessage, nextSendQueue=" + queueName);
      if (queueName == null)
        throw new QueueException("Unable to select a physical destination queue for this clustered queue!");
      try
      {
        cTxId.putMessage(queueName, message);
      } catch (Exception e)
      {
        throw new QueueException(e.toString());
      }
    } else
    {
      try
      {
        cTxId.putMessage(message);
      } catch (Exception e)
      {
        throw new QueueException(e.toString());
      }
    }
  }

  public void commit(Object txId) throws QueueException
  {
    if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), toString() + "/commit, txId=" + txId);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    ((ClusteredQueueFlowController) getFlowController()).setLastDelay(cTxId.commit());
  }

  public void commit(final Object txId, AsyncCompletionCallback callback)
  {
    if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), toString() + "/commit (callback), txId=" + txId);
    final ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    cTxId.commit(new AsyncCompletionCallback(callback)
    {
      public synchronized void done(boolean success)
      {
        Long fcDelay = (Long) getResult();
        if (fcDelay != null)
        {
          if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), toString() + "/commit (callback), txId=" + txId + ", newDelay=" + fcDelay);
          ((ClusteredQueueFlowController) getFlowController()).setLastDelay(fcDelay.longValue());
        }
      }
    });
  }

  public void rollback(Object txId, boolean b) throws QueueException
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/rollback, txId=" + txId + ", b=" + b);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    cTxId.rollback(b);
  }

  public void rollback(Object txId, boolean b, AsyncCompletionCallback callback)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/rollback, txId=" + txId + ", b=" + b);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    cTxId.rollback(b, callback);
  }

  public void prepare(Object txId, XidImpl xid) throws QueueException
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/prepare, txId=" + txId + ", xid=" + xid);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    cTxId.prepare(xid);
  }

  public void commit(Object txId, XidImpl xid) throws QueueException
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/commit, txId=" + txId + ", xid=" + xid);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    ((ClusteredQueueFlowController) getFlowController()).setLastDelay(cTxId.commit(xid));
  }

  public void rollback(Object txId, XidImpl xid, boolean b) throws QueueException
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(getQueueName(), toString() + "/commit, txId=" + txId + ", xid=" + xid + ", b=" + b);
    ClusteredTransactionId cTxId = (ClusteredTransactionId) txId;
    cTxId.rollback(xid, b);
  }

  public void deleteContent() throws QueueException
  {
  }

  public void startQueue() throws QueueException
  {
  }

  public void stopQueue() throws QueueException
  {
  }

  public String toString()
  {
    return "clustered queue, name=" + getQueueName();
  }
}
