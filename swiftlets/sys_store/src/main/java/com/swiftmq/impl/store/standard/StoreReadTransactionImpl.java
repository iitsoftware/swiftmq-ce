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
import com.swiftmq.swiftlet.store.StoreException;
import com.swiftmq.swiftlet.store.StoreReadTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

import java.io.IOException;
import java.util.ArrayList;

public class StoreReadTransactionImpl extends StoreTransactionImpl
        implements StoreReadTransaction {
    boolean markRedelivered = false;

    StoreReadTransactionImpl(StoreContext ctx, String queueName, QueueIndex queueIndex, boolean markRedelivered) {
        super(ctx, queueName, queueIndex);
        this.markRedelivered = markRedelivered;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/create...");
    }

    public void remove(Object key)
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove, key=" + key);
        if (closed)
            throw new StoreException("Transaction is closed");
        keys.add(key);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/remove done, key=" + key);
    }

    public void prepare(XidImpl globalTxId) throws StoreException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/prepare, globalTxId=" + globalTxId);
        try {
            prepareLogRecord = new PrepareLogRecordImpl(PrepareLogRecordImpl.READ_TRANSACTION, queueName, globalTxId, keys);
            ctx.preparedLog.add(prepareLogRecord);
        } catch (IOException e) {
            throw new StoreException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/prepare, globalTxId=" + globalTxId + ", done");
    }

    public void commit(XidImpl globalTxId) throws StoreException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/commit, globalTxId: " + globalTxId);
        try {
            ctx.preparedLog.remove(prepareLogRecord);
            commit();
        } catch (IOException e) {
            throw new StoreException(e.toString());
        }
        prepareLogRecord = null;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/commit, globalTxId: " + globalTxId + ", done");
    }

    public void commit()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...");
        if (closed)
            throw new StoreException("Transaction is closed");
        txId = ctx.transactionManager.createTxId();
        journal = new ArrayList();
        queueIndex.setJournal(journal);
        try {
            for (int i = 0; i < keys.size(); i++) {
                addMessagePageReference(queueIndex.remove((QueueIndexEntry) keys.get(i)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new StoreException(e.getMessage());
        }
        keys.clear();
        super.commit();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...done.");
    }

    public void commit(AsyncCompletionCallback callback) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...");
        AsyncCompletionCallback localCallback = createLocalCallback(callback);
        if (checkClosedAsync(localCallback))
            return;
        txId = ctx.transactionManager.createTxId();
        journal = new ArrayList();
        queueIndex.setJournal(journal);
        try {
            for (int i = 0; i < keys.size(); i++) {
                addMessagePageReference(queueIndex.remove((QueueIndexEntry) keys.get(i)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            localCallback.setException(e);
            localCallback.notifyCallbackStack(false);
            return;
        }
        keys.clear();
        if (journal != null && journal.size() > 0) {
            try {
                ctx.recoveryManager.commit(new CommitLogRecord(txId, null, journal, this, localCallback, messagePageRefs));
            } catch (Exception e) {
                localCallback.setException(new StoreException(e.toString()));
                localCallback.notifyCallbackStack(false);
            }
        } else
            localCallback.notifyCallbackStack(true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/commit...done.");
    }

    public void abort(XidImpl globalTxId) throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort, globalTxId: " + globalTxId);
        if (prepareLogRecord != null) {
            try {
                ctx.preparedLog.remove(prepareLogRecord);
            } catch (IOException e) {
                throw new StoreException(e.toString());
            }
            prepareLogRecord = null;
        }
        abort();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$store", toString() + "/abort, globalTxId: " + globalTxId + ", done");
    }

    public void abort()
            throws StoreException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...");
        txId = ctx.transactionManager.createTxId();
        journal = new ArrayList();
        queueIndex.setJournal(journal);
        try {
            if (markRedelivered) {
                for (int i = 0; i < keys.size(); i++) {
                    queueIndex.incDeliveryCount((QueueIndexEntry) keys.get(i));
                }
            }
        } catch (Exception e) {
            throw new StoreException(e.getMessage());
        }
        keys.clear();
        // don't wonder, we are committing the redelivered settings
        super.commit();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/abort...done.");
    }

    protected void close() {
        super.close();
    }

    public String toString() {
        return "[StoreReadTransactionImpl, " + super.toString() + "]";
    }
}

