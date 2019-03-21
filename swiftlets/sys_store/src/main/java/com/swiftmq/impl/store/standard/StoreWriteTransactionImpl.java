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

import com.swiftmq.impl.store.standard.index.QueueIndex;
import com.swiftmq.impl.store.standard.index.QueueIndexEntry;
import com.swiftmq.impl.store.standard.log.CommitLogRecord;
import com.swiftmq.impl.store.standard.xa.PrepareLogRecordImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.swiftlet.store.StoreException;
import com.swiftmq.swiftlet.store.StoreWriteTransaction;
import com.swiftmq.tools.concurrent.Semaphore;

import java.io.IOException;
import java.util.ArrayList;

public class StoreWriteTransactionImpl extends StoreTransactionImpl
    implements StoreWriteTransaction
{

  StoreWriteTransactionImpl(StoreContext ctx, String queueName, QueueIndex queueIndex)
  {
    super(ctx, queueName, queueIndex);
    txId = ctx.transactionManager.createTxId();
    journal = new ArrayList();
    queueIndex.setJournal(journal);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create...");
  }

  /**
   * @param storeEntry
   * @throws com.swiftmq.swiftlet.store.StoreException
   *
   */
  public void insert(StoreEntry storeEntry)
      throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/insert, storeEntry=" + storeEntry);
    if (closed)
      throw new StoreException("Transaction is closed");
    try
    {
      keys.add(queueIndex.add(storeEntry));
    } catch (Exception e)
    {
      e.printStackTrace();
      throw new StoreException(e.getMessage());
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/insert done, storeEntry=" + storeEntry);
  }

  public void prepare(XidImpl globalTxId) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/prepare, globalTxId=" + globalTxId);
    try
    {
      prepareLogRecord = new PrepareLogRecordImpl(PrepareLogRecordImpl.WRITE_TRANSACTION, queueName, globalTxId, keys);
      ctx.preparedLog.add(prepareLogRecord);
      ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, null));
      sem.waitHere();
      ctx.transactionManager.removeTxId(txId);
    } catch (Exception e)
    {
      throw new StoreException(e.getMessage());
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/prepare, globalTxId=" + globalTxId + ", done");
  }

  public void commit(XidImpl globalTxId) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit, globalTxId: " + globalTxId);
    try
    {
      ctx.preparedLog.remove(prepareLogRecord);
    } catch (IOException e)
    {
      throw new StoreException(e.toString());
    }
    prepareLogRecord = null;
    close();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/commit, globalTxId: " + globalTxId + ", done");
  }

  public void abort(XidImpl globalTxId) throws StoreException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort, globalTxId: " + globalTxId);
    txId = ctx.transactionManager.createTxId();
    sem = new Semaphore();
    journal = new ArrayList();
    queueIndex.setJournal(journal);
    try
    {
      for (int i = 0; i < keys.size(); i++)
      {
        addMessagePageReference(queueIndex.remove((QueueIndexEntry) keys.get(i)));
      }
      ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, messagePageRefs));
      sem.waitHere();
      ctx.transactionManager.removeTxId(txId);
    } catch (Exception e)
    {
      throw new StoreException(e.toString());
    }
    if (prepareLogRecord != null)
    {
      try
      {
        ctx.preparedLog.remove(prepareLogRecord);
      } catch (IOException e)
      {
        throw new StoreException(e.toString());
      }
      prepareLogRecord = null;
    }
    close();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/abort, globalTxId: " + globalTxId + ", done");
  }

  protected void close()
  {
    super.close();
  }

  public String toString()
  {
    return "[StoreWriteTransactionImpl, " + super.toString() + "]";
  }
}

