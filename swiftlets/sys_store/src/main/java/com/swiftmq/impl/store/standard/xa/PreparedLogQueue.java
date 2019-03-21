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

package com.swiftmq.impl.store.standard.xa;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.CacheReleaseListener;
import com.swiftmq.impl.store.standard.index.QueueIndex;
import com.swiftmq.impl.store.standard.index.QueueIndexEntry;
import com.swiftmq.impl.store.standard.log.CommitLogRecord;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.swiftlet.store.StoreEntry;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PreparedLogQueue extends PreparedLog implements CacheReleaseListener
{
  StoreContext ctx = null;
  DataByteArrayInputStream inStream = null;
  DataByteArrayOutputStream outStream = null;
  volatile QueueIndex queueIndex = null;
  ArrayList cache = null;

  public PreparedLogQueue(StoreContext ctx, QueueIndex queueIndex) throws Exception
  {
    this.ctx = ctx;
    this.queueIndex = queueIndex;
    inStream = new DataByteArrayInputStream();
    outStream = new DataByteArrayOutputStream(1024);
    cache = new ArrayList();
    preload();
    ctx.logSwiftlet.logInformation("sys$store", toString() + "/created, " + cache.size() + " prepared transactions");
  }

  public void releaseCache()
  {
    try
    {
      queueIndex.unloadPages();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void preload() throws Exception
  {
    List qiEntries = queueIndex.getEntries();
    queueIndex.unloadPages();
    for (int i = 0; i < qiEntries.size(); i++)
    {
      QueueIndexEntry entry = (QueueIndexEntry) qiEntries.get(i);
      StoreEntry storeEntry = queueIndex.get(entry);
      BytesMessageImpl msg = (BytesMessageImpl) storeEntry.message;
      byte[] b = new byte[(int) msg.getBodyLength()];
      msg.readBytes(b);
      inStream.setBuffer(b, 0, b.length);
      PrepareLogRecordImpl logRecord = new PrepareLogRecordImpl(0);
      logRecord.readContent(inStream);
      CacheEntry cacheEntry = new CacheEntry();
      cacheEntry.logRecord = logRecord;
      long address = (long) ArrayListTool.setFirstFreeOrExpand(cache, cacheEntry);
      logRecord.setAddress(address);
      cacheEntry.indexEntry = entry;
    }
    queueIndex.unloadPages();
  }

  public synchronized long add(PrepareLogRecordImpl logRecord) throws IOException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/add, logRecord: " + logRecord);
    CacheEntry cacheEntry = new CacheEntry();
    cacheEntry.logRecord = logRecord;
    long address = (long) ArrayListTool.setFirstFreeOrExpand(cache, cacheEntry);
    logRecord.setAddress(address);
    outStream.rewind();
    logRecord.writeContent(outStream);
    try
    {
      BytesMessageImpl msg = new BytesMessageImpl();
      msg.writeBytes(outStream.getBuffer(), 0, outStream.getCount());
      StoreEntry storeEntry = new StoreEntry();
      storeEntry.message = msg;
      long txId = ctx.transactionManager.createTxId(false);
      List journal = new ArrayList();
      queueIndex.setJournal(journal);
      cacheEntry.indexEntry = queueIndex.add(storeEntry);
      Semaphore sem = new Semaphore();
      ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, null));
      sem.waitHere();
      ctx.transactionManager.removeTxId(txId);
    } catch (Exception e)
    {
      throw new IOException(e.toString());
    }
    return address;
  }

  public synchronized PrepareLogRecordImpl get(long address) throws IOException
  {
    CacheEntry cacheEntry = (CacheEntry) cache.get((int) address);
    if (cacheEntry == null)
      throw new EOFException("No CacheEntry found at index: " + address);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$store", toString() + "/get, logRecord: " + cacheEntry.logRecord);
    return cacheEntry.logRecord;
  }

  public synchronized List getAll() throws IOException
  {
    List al = new ArrayList();
    for (int i = 0; i < cache.size(); i++)
    {
      CacheEntry cacheEntry = (CacheEntry) cache.get(i);
      if (cacheEntry != null)
        al.add(cacheEntry.logRecord);
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getAll, logRecords: " + al);
    return al;
  }

  public synchronized void remove(PrepareLogRecordImpl logRecord) throws IOException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, logRecord: " + logRecord);
    int address = (int) logRecord.getAddress();
    CacheEntry cacheEntry = (CacheEntry) cache.get(address);
    if (cacheEntry == null)
      throw new EOFException("No CacheEntry found at index: " + address);
    cache.set(address, null);
    long txId = ctx.transactionManager.createTxId(false);
    List journal = new ArrayList();
    queueIndex.setJournal(journal);
    Semaphore sem = new Semaphore();
    try
    {
      queueIndex.remove(cacheEntry.indexEntry);
      ctx.recoveryManager.commit(new CommitLogRecord(txId, sem, journal, this, null));
    } catch (Exception e)
    {
      throw new IOException(e.toString());
    }
    sem.waitHere();
    ctx.transactionManager.removeTxId(txId);
  }

  public boolean backupRequired()
  {
    return false;
  }

  public void backup(String destPath) throws Exception
  {
    // Nothing to do
  }

  public String toString()
  {
    return "PreparedLogQueue, queueIndex=" + queueIndex;
  }

  private class CacheEntry
  {
    PrepareLogRecordImpl logRecord = null;
    QueueIndexEntry indexEntry = null;

    public String toString()
    {
      return "[CacheEntry, logRecord=" + logRecord + ", indexEntry=" + indexEntry + "]";
    }
  }
}
