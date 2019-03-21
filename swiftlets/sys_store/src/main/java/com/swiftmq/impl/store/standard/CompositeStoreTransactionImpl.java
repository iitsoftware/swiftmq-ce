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
import com.swiftmq.impl.store.standard.index.QueueIndexEntry;
import com.swiftmq.impl.store.standard.log.AbortLogRecord;
import com.swiftmq.impl.store.standard.log.CommitLogRecord;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.swiftlet.store.PersistentStore;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.swiftlet.store.StoreException;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.ArrayList;
import java.util.List;

public class CompositeStoreTransactionImpl extends CompositeStoreTransaction implements CacheReleaseListener
{
  StoreContext ctx = null;
  PersistentStore persistentStore = null;
  QueueIndex currentQueueIndex = null;
  List<QueueIndex> queueIndexes = null;
  long txId = -1;
  List<RemovedKeyEntry> keysRemoved = null;
  List<QueueIndexEntry> keysInserted = null;
  List journal = null;
  Semaphore sem = null;
  boolean closed = false;
  boolean markRedelivered = false;
  boolean referencable = true;

  public CompositeStoreTransactionImpl(StoreContext ctx)
  {
    this.ctx = ctx;
    queueIndexes = new ArrayList<QueueIndex>();
    journal = new ArrayList();
    sem = new Semaphore();
  }

  public void setReferencable(boolean referencable)
  {
    this.referencable = referencable;
  }

  public boolean isReferencable()
  {
    return referencable;
  }

  public void setMarkRedelivered(boolean markRedelivered)
  {
    this.markRedelivered = markRedelivered;
  }

  protected void checkClosedAsync(AsyncCompletionCallback callback)
  {
    if (closed)
    {
      callback.setException(new StoreException("Transaction is closed"));
      callback.notifyCallbackStack(false);
    }
  }

  protected AsyncCompletionCallback createLocalCallback(AsyncCompletionCallback callback)
  {
    return new AsyncCompletionCallback(callback)
    {
      public synchronized void done(boolean success)
      {
        // Empty tx. May happen when duplicates where detected
        removeTxId();
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
      for (int i = 0; i < queueIndexes.size(); i++)
        queueIndexes.get(i).unloadPages();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void remove(Object key) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, key=" + key);
    if (closed)
      throw new StoreException("Transaction is closed");
    if (keysRemoved == null)
      keysRemoved = new ArrayList<RemovedKeyEntry>();
    keysRemoved.add(new RemovedKeyEntry(currentQueueIndex, (QueueIndexEntry) key));
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove done, key=" + key);
  }

  public void insert(StoreEntry storeEntry) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/insert, storeEntry=" + storeEntry);
    if (closed)
      throw new StoreException("Transaction is closed");
    if (txId == -1)
      txId = ctx.transactionManager.createTxId();      // to avoid a deadlock
    if (keysInserted == null)
      keysInserted = new ArrayList<QueueIndexEntry>();
    try
    {
      keysInserted.add(currentQueueIndex.add(storeEntry, referencable));
    } catch (Exception e)
    {
      e.printStackTrace();
      throw new StoreException(e.getMessage());
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/insert done, storeEntry=" + storeEntry);
  }

  public void prepare(XidImpl xid) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/prepare, NOP");
  }

  public void commit(XidImpl xid) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (2PC), NOP");
  }

  public void commit() throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit, NOP");
  }

  public void commit(AsyncCompletionCallback asyncCompletionCallback)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (async), NOP");
  }

  public void abort(XidImpl xid) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort (2PC), NOP");
  }

  public void abort() throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort, NOP");
  }

  public void abort(AsyncCompletionCallback asyncCompletionCallback)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort (async), NOP");
  }

  public void setPersistentStore(PersistentStore persistentStore) throws StoreException
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/setPersistentStore, persistentStore=" + persistentStore);
    this.persistentStore = persistentStore;
    if (persistentStore != null)
    {
      currentQueueIndex = ((PersistentStoreImpl) persistentStore).getQueueIndex();
      queueIndexes.add(currentQueueIndex);
      currentQueueIndex.setJournal(journal);
    }
  }

  private List<MessagePageReference> processRemovedKeys() throws Exception
  {
    List<MessagePageReference> messagePageRefs = null;
    if (keysRemoved != null)
    {
      for (int i = 0; i < keysRemoved.size(); i++)
      {
        RemovedKeyEntry entry = keysRemoved.get(i);
        entry.queueIndex.setJournal(journal);
        MessagePageReference ref = entry.queueIndex.remove(entry.key);
        if (ref != null)
        {
          if (messagePageRefs == null)
            messagePageRefs = new ArrayList<MessagePageReference>();
          messagePageRefs.add(ref);
        }
      }
    }
    return messagePageRefs;
  }

  public void commitTransaction() throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commitTransaction...");
    if (closed)
      throw new StoreException("Transaction is closed");
    if (txId == -1)
      txId = ctx.transactionManager.createTxId();
    try
    {
      List<MessagePageReference> messagePageRefs = processRemovedKeys();
      if (journal != null && journal.size() > 0)
      {
        ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, messagePageRefs));
        sem.waitHere();
        removeTxId();
      } else
        removeTxId();
      close();
    } catch (Exception e)
    {
      throw new StoreException(e.toString());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commitTransaction...done.");
  }

  private void removeTxId()
  {
    if (txId != -1)
    {
      ctx.transactionManager.removeTxId(txId);
      txId = -1;
    }
  }

  public void commitTransaction(AsyncCompletionCallback callback)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (callback) ...");
    AsyncCompletionCallback localCallback = createLocalCallback(callback);
    checkClosedAsync(localCallback);
    if (localCallback.isNotified())
      return;
    try
    {
      List<MessagePageReference> messagePageRefs = processRemovedKeys();
      if (journal != null && journal.size() > 0)
        ctx.recoveryManager.commit(new CommitLogRecord(txId, null, journal, this, localCallback, messagePageRefs));
      else
      {
        localCallback.notifyCallbackStack(true);
        removeTxId();
      }
    } catch (Exception e)
    {
      localCallback.setException(new StoreException(e.toString()));
      localCallback.notifyCallbackStack(false);
      removeTxId();
    }

    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit (callback) ... done.");
  }

  public void abortTransaction() throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abortTransaction...");
    if (closed)
      throw new StoreException("Transaction is closed");
    try
    {
      if (journal != null && journal.size() > 0)
      {
        if (keysRemoved != null && markRedelivered)
          ctx.recoveryManager.abort(new AbortLogRecord(txId, null, journal, null));
        else
        {
          ctx.recoveryManager.abort(new AbortLogRecord(txId, sem, journal, this));
          sem.waitHere();
        }
      }
      if (keysRemoved != null && markRedelivered)
      {
        List newJournal = new ArrayList();
        for (int i = 0; i < keysRemoved.size(); i++)
        {
          RemovedKeyEntry entry = keysRemoved.get(i);
          entry.queueIndex.setJournal(newJournal);
          entry.queueIndex.incDeliveryCount(entry.key);
        }
        // don't wonder, we are committing the redelivered settings
        ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, newJournal, this, null));
        sem.waitHere();
      }
      removeTxId();
      close();
    } catch (Exception e)
    {
      throw new StoreException(e.getMessage());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abortTransaction...done.");
  }

  public void abortTransaction(AsyncCompletionCallback callback)
  {
    AsyncCompletionCallback localCallback = createLocalCallback(callback);
    checkClosedAsync(localCallback);
    if (localCallback.isNotified())
      return;
    boolean doNotify = true;
    if (journal != null && journal.size() > 0)
    {
      try
      {
        doNotify = false;
        if (keysRemoved != null && markRedelivered)
          ctx.recoveryManager.abort(new AbortLogRecord(txId, null, journal, this, null));
        else
          ctx.recoveryManager.abort(new AbortLogRecord(txId, null, journal, this, localCallback));
      } catch (Exception e)
      {
        localCallback.setException(new StoreException(e.toString()));
        localCallback.notifyCallbackStack(false);
        removeTxId();
        return;
      }
    }
    if (keysRemoved != null && markRedelivered)
    {
      List newJournal = new ArrayList();
      try
      {
        for (int i = 0; i < keysRemoved.size(); i++)
        {
          RemovedKeyEntry entry = keysRemoved.get(i);
          entry.queueIndex.setJournal(newJournal);
          entry.queueIndex.incDeliveryCount(entry.key);
        }
        doNotify = false;
        // don't wonder, we are committing the redelivered settings
        ctx.recoveryManager.commit(new CommitLogRecord(txId, null, newJournal, this, localCallback, null));
      } catch (Exception e)
      {
        localCallback.setException(new StoreException(e.toString()));
        localCallback.notifyCallbackStack(false);
        removeTxId();
        return;
      }
    }
    if (doNotify)
      callback.notifyCallbackStack(true);
  }

  protected void close()
  {
    if (journal != null)
      journal.clear(); // GC
    if (keysInserted != null)
      keysInserted.clear();
    if (keysRemoved != null)
      keysRemoved.clear();
    if (queueIndexes != null)
      queueIndexes.clear();
    currentQueueIndex = null;
    closed = true;
  }

  public String toString()
  {
    return "[CompositeStoreTransactionImpl, persistentStore=" + persistentStore + "]";
  }

  private class RemovedKeyEntry
  {
    QueueIndex queueIndex;
    QueueIndexEntry key;

    private RemovedKeyEntry(QueueIndex queueIndex, QueueIndexEntry key)
    {
      this.queueIndex = queueIndex;
      this.key = key;
    }
  }
}
