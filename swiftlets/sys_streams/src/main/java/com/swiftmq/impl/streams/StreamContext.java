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

package com.swiftmq.impl.streams;

import com.swiftmq.impl.streams.comp.message.MessageBuilder;
import com.swiftmq.impl.streams.processor.StreamProcessor;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.util.IdGenerator;
import org.graalvm.polyglot.Value;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class StreamContext {
    public SwiftletContext ctx;
    public Stream stream;
    public StreamProcessor streamProcessor;
    public MessageBuilder messageBuilder;
    public Value bindings;
    public Entity entity;
    public Entity usage;
    public String msgIdPrefix;
    public ClassLoader classLoader = null;
    long msgInc = 0;
    List<Entry> queueTransactions;
    List<TransactionFlushListener> flushListeners;
    Exception lastException;

    public StreamContext(SwiftletContext ctx, Entity entity) {
        this.ctx = ctx;
        this.entity = entity;
        msgIdPrefix = IdGenerator.getInstance().nextId('/') + "/" + SwiftletManager.getInstance().getRouterName() + "/streams/" + entity.getName() + "/" + System.currentTimeMillis() + "/";
        queueTransactions = new ArrayList<Entry>();
        flushListeners = new ArrayList<TransactionFlushListener>();
    }

    public String nextId() {
        StringBuffer b = new StringBuffer(msgIdPrefix);
        b.append(msgInc++);
        if (msgInc == Long.MAX_VALUE)
            msgInc = 0;
        return b.toString();
    }

    public void setLastException(Exception lastException) {
        this.lastException = lastException;
    }

    public Exception getLastException() {
        return lastException;
    }

    public void logStackTrace(Exception e) {
        StringWriter w = new StringWriter();
        PrintWriter p = new PrintWriter(w);
        p.println("Exception occured:");
        e.printStackTrace(p);
        p.flush();
        stream.log().error(w.getBuffer().toString());
    }

    public void addTransactionFlushListener(TransactionFlushListener listener) {
        flushListeners.add(listener);
    }

    public void removeTransactionFlushListener(TransactionFlushListener listener) {
        flushListeners.remove(listener);
    }

    private void notifyTransactionFlushListeners() {
        for (int i = 0; i < flushListeners.size(); i++)
            flushListeners.get(i).flush();
    }

    public void addTransaction(QueueTransaction transaction, TransactionFinishListener finishedListener) {
        queueTransactions.add(new Entry(transaction, finishedListener));
    }

    private void lock() {
        for (int i = 0; i < queueTransactions.size(); i++) {
            queueTransactions.get(i).transaction.lockQueue();
        }
    }

    private void unlock() {
        for (int i = 0; i < queueTransactions.size(); i++) {
            queueTransactions.get(i).transaction.unlockQueue(false);
        }
    }

    public void commitTransactions() throws Exception {
        if (ctx.queueManager.isUseGlobaLocking())
            commitTransactionsGlobalLock();
        else
            commitTransactionsNoGlobalLock();
        stream.deferredClose();
    }

    public void commitTransactionsGlobalLock() throws Exception {
        notifyTransactionFlushListeners();
        CompositeStoreTransaction compTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
        compTx.setReferencable(false);
        try {
            ctx.queueManager.lockMultiQueue();
            lock();
            for (int i = 0; i < queueTransactions.size(); i++) {
                Entry entry = queueTransactions.get(i);
                entry.transaction.setCompositeStoreTransaction(compTx);
                entry.transaction.commit();
                entry.transaction.setCompositeStoreTransaction(null);
            }
            compTx.commitTransaction();
        } finally {
            try {
                unlock();
            } catch (Exception e) {

            }
            ctx.queueManager.unlockMultiQueue();
        }
        for (int i = 0; i < queueTransactions.size(); i++) {
            Entry entry = queueTransactions.get(i);
            if (entry.listener != null)
                entry.listener.transactionFinished();
        }
        queueTransactions.clear();
    }

    public void commitTransactionsNoGlobalLock() throws Exception {
        notifyTransactionFlushListeners();

        for (int i = 0; i < queueTransactions.size(); i++) {
            Entry entry = queueTransactions.get(i);
            entry.transaction.commit();
            if (entry.listener != null)
                entry.listener.transactionFinished();
        }
        queueTransactions.clear();
    }

    public void rollbackTransactions() {
        notifyTransactionFlushListeners();
        for (int i = 0; i < queueTransactions.size(); i++)
            try {
                Entry entry = queueTransactions.get(i);
                entry.transaction.rollback();
                if (entry.listener != null)
                    entry.listener.transactionFinished();
            } catch (QueueException e) {

            }
        queueTransactions.clear();
        stream.deferredClose();
    }

    private class Entry {
        QueueTransaction transaction;
        TransactionFinishListener listener;

        public Entry(QueueTransaction transaction, TransactionFinishListener listener) {
            this.transaction = transaction;
            this.listener = listener;
        }
    }
}
