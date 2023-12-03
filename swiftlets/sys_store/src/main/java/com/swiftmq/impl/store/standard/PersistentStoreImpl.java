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
import com.swiftmq.impl.store.standard.index.RootIndex;
import com.swiftmq.swiftlet.store.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PersistentStoreImpl implements PersistentStore {
    StoreContext ctx = null;
    String queueName = null;
    RootIndex rootIndex = null;
    QueueIndex queueIndex = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    final AtomicBoolean deleted = new AtomicBoolean(false);

    PersistentStoreImpl(StoreContext ctx, QueueIndex queueIndex, RootIndex rootIndex, String queueName) {
        this.ctx = ctx;
        this.queueIndex = queueIndex;
        this.rootIndex = rootIndex;
        this.queueName = queueName;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create...");
    }

    public QueueIndex getQueueIndex() {
        return queueIndex;
    }

    public List<StoreEntry> getStoreEntries()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/getStoreEntries...");
        if (closed.get())
            throw new StoreException("Store is closed");
        List<StoreEntry> entries = null;
        try {
            List qiEntries = queueIndex.getEntries();
            queueIndex.unloadPages();
            entries = new ArrayList(qiEntries.size());
            for (int i = 0; i < qiEntries.size(); i++) {
                QueueIndexEntry entry = (QueueIndexEntry) qiEntries.get(i);
                StoreEntry storeEntry = new StoreEntry();
                storeEntry.key = entry;
                storeEntry.priority = entry.getPriority();
                storeEntry.deliveryCount = entry.getDeliveryCount();
                storeEntry.expirationTime = entry.getExpirationTime();
                storeEntry.message = null;
                entries.add(storeEntry);
            }
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/getStoreEntries done, entries.size()=" + entries.size());
        return entries;
    }

    public StoreEntry get(Object key)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get, key=" + key);
        if (closed.get())
            throw new StoreException("Store is closed");
        StoreEntry entry = null;
        try {
            entry = queueIndex.get((QueueIndexEntry) key);
        } catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/get one, entry=" + entry);
        return entry;
    }

    public StoreReadTransaction createReadTransaction(boolean markRedelivered)
            throws StoreException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/createReadTransaction, markRedelivered=" + markRedelivered);
        return new StoreReadTransactionImpl(ctx, queueName, queueIndex, markRedelivered);
    }

    public StoreWriteTransaction createWriteTransaction()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createWriteTransaction");
        return new StoreWriteTransactionImpl(ctx, queueName, queueIndex);
    }

    public void delete()
            throws StoreException {
        if (deleted.get())
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/delete...");
        try {
            rootIndex.deleteQueueIndex(queueName, queueIndex);
            deleted.set(true);
        } catch (Exception e) {
            throw new StoreException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/delete...done");
    }

    public void close()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/close");
        closed.set(true);
    }

    public String toString() {
        return "[PersistentStoreImpl, queueName=" + queueName + ", queueIndex=" + queueIndex + "]";
    }
}

