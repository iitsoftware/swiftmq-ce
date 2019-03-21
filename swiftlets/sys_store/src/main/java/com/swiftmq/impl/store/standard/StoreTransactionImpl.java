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

package com.swiftmq.impl.store.standard;

import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.impl.store.standard.index.MessagePageReference;
import com.swiftmq.impl.store.standard.index.QueueIndex;
import com.swiftmq.impl.store.standard.log.AbortLogRecord;
import com.swiftmq.impl.store.standard.log.CommitLogRecord;
import com.swiftmq.impl.store.standard.xa.PrepareLogRecordImpl;
import com.swiftmq.swiftlet.store.StoreException;
import com.swiftmq.swiftlet.store.StoreTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.ArrayList;
import java.util.List;

public abstract class StoreTransactionImpl implements StoreTransaction, CacheReleaseListener
{
  StoreContext ctx = null;
  String queueName = null;
  QueueIndex queueIndex = null;
  Semaphore sem = null;
  List journal = null;
  List keys = null;
  List messagePageRefs = null;
  boolean closed = false;
  PrepareLogRecordImpl prepareLogRecord = null;
  long txId = -1;

  public StoreTransactionImpl(StoreContext ctx, String queueName, QueueIndex queueIndex)
  {
    this.ctx = ctx;
    this.queueName = queueName;
    this.queueIndex = queueIndex;
    sem = new Semaphore();
    keys = new ArrayList();
  }

  protected void addMessagePageReference(MessagePageReference ref)
  {
    if (ref != null)
    {
      if (messagePageRefs == null)
        messagePageRefs = new ArrayList();
      messagePageRefs.add(ref);
    }
  }

  protected boolean checkClosedAsync(AsyncCompletionCallback callback)
  {
    if (closed)
    {
      callback.setException(new StoreException("Transaction is closed"));
      callback.notifyCallbackStack(false);
    }
    return closed;
  }

  protected AsyncCompletionCallback createLocalCallback(AsyncCompletionCallback callback)
  {
    return new AsyncCompletionCallback(callback)
    {
      public synchronized void done(boolean success)
      {
        ctx.transactionManager.removeTxId(txId);
        close();
        if (!success)
          next.setException(getException());
      }
    };
  }

  public synchronized void releaseCache()
  {
    try
    {
      queueIndex.unloadPages();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void commit()
      throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...");
    if (closed)
      throw new StoreException("Transaction is closed");
    try
    {
      if (journal != null && journal.size() > 0)
      {
        ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, null, messagePageRefs));
        sem.waitHere();
      }
      ctx.transactionManager.removeTxId(txId);
      close();
    } catch (Exception e)
    {
      throw new StoreException(e.toString());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...done.");
  }

  public void commit(AsyncCompletionCallback callback)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (callback) ...");
    AsyncCompletionCallback localCallback = createLocalCallback(callback);
    if (checkClosedAsync(localCallback))
      return;
    if (journal != null && journal.size() > 0)
    {
      try
      {
        ctx.recoveryManager.commit(new CommitLogRecord(txId, null, journal, this, localCallback, messagePageRefs));
      } catch (Exception e)
      {
        localCallback.setException(new StoreException(e.toString()));
        localCallback.notifyCallbackStack(false);
      }
    } else
      localCallback.notifyCallbackStack(true);

    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (callback) ... done.");
  }

  public void abort()
      throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...");
    if (closed)
      throw new StoreException("Transaction is closed");
    try
    {
      if (journal != null && journal.size() > 0)
      {
        ctx.recoveryManager.abort(new AbortLogRecord(txId, sem, journal, this, null));
        sem.waitHere();
      }
      ctx.transactionManager.removeTxId(txId);
      close();
    } catch (Exception e)
    {
      throw new StoreException(e.getMessage());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...done.");
  }

  public void abort(AsyncCompletionCallback callback)
  {
    AsyncCompletionCallback localCallback = createLocalCallback(callback);
    if (checkClosedAsync(localCallback))
      return;
    if (localCallback.isNotified())
      return;
    if (journal != null && journal.size() > 0)
    {
      try
      {
        ctx.recoveryManager.abort(new AbortLogRecord(txId, null, journal, this, localCallback));
      } catch (Exception e)
      {
        localCallback.setException(new StoreException(e.toString()));
        localCallback.notifyCallbackStack(false);
      }
    } else
      callback.notifyCallbackStack(true);
  }

  protected void close()
  {
    if (journal != null)
      journal.clear(); // GC
    if (keys != null)
      keys.clear();
    closed = true;
  }

  public String toString()
  {
    return "queueIndex=" + queueIndex;
  }
}

