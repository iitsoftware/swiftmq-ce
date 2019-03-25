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
import com.swiftmq.swiftlet.queue.QueueTransaction;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransactionManager {
    SessionContext ctx = null;
    List transactionFactories = new ArrayList();
    List transactions = new ArrayList();

    TransactionManager(SessionContext ctx) {
        this.ctx = ctx;
    }

    void addTransactionFactory(TransactionFactory transactionFactory) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/addTransactionFactory, transactionFactory=" + transactionFactory);
        transactionFactories.add(transactionFactory);
        transactions.add(new Pair(transactionFactory.createTransaction(), transactionFactory));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/addTransactionFactory done, transactionFactory=" + transactionFactory);
    }

    void removeTransactionFactory(TransactionFactory transactionFactory) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/removeTransactionFactory, transactionFactory=" + transactionFactory);
        transactionFactories.remove(transactionFactory);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/removeTransactionFactory done, transactionFactory=" + transactionFactory);
    }

    void startTransactions() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions");
        transactions.clear();
        for (Iterator iter = transactionFactories.iterator(); iter.hasNext(); ) {
            TransactionFactory f = (TransactionFactory) iter.next();
            if (!f.isMarkedForClose()) {
                try {
                    QueueTransaction t = f.createTransaction();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions, add=" + t + ", closed=" + t.isClosed());
                    transactions.add(new Pair(t, f));
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions, e=" + e + ", remove transaction factory.");
                    iter.remove();
                }
            } else
                iter.remove();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/startTransactions done");
    }

    private void lock() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/lock ...");
        for (int i = 0; i < transactions.size(); i++) {
            Pair p = (Pair) transactions.get(i);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/lock, queue=" + p.tx.getQueueName());
            p.tx.lockQueue();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/lock done");
    }

    private void unlock() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/unlock ...");
        for (int i = 0; i < transactions.size(); i++) {
            Pair p = (Pair) transactions.get(i);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/unlock, queue=" + p.tx.getQueueName());
            p.tx.unlockQueue(false);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/unlock done");
    }

    private void commitWithGlobalLock() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithGlobalLock, transactions.size=" + transactions.size());
        CompositeStoreTransaction compTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
        compTx.setReferencable(false);
        ctx.queueManager.lockMultiQueue();
        lock();
        try {
            for (Iterator iter = transactions.iterator(); iter.hasNext(); ) {
                Pair p = (Pair) iter.next();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithGlobalLock, t=" + p.tx + ", closed=" + p.tx.isClosed());
                DestinationCollector c = p.factory.getCollector();
                if (c != null)
                    c.commit();
                try {
                    p.tx.setCompositeStoreTransaction(compTx);
                    p.tx.commit();
                    p.tx.setCompositeStoreTransaction(null);
                } catch (QueueTransactionClosedException e) {
                    // ignore
                    // Happens when temp queues have been deleted meanwhile
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithGlobalLock, t=" + p.tx + ", QueueTransactionClosedException, remove");
                    iter.remove();
                    p.tx.unlockQueue(false);
                }
            }
            compTx.commitTransaction();
        } finally {
            unlock();
            ctx.queueManager.unlockMultiQueue();
            startTransactions();
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithGlobalLock done");
    }

    private void commitWithoutGlobalLock() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithoutGlobalLock, transactions.size=" + transactions.size());
        for (Iterator iter = transactions.iterator(); iter.hasNext(); ) {
            Pair p = (Pair) iter.next();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithoutGlobalLock, t=" + p.tx + ", closed=" + p.tx.isClosed());
            DestinationCollector c = p.factory.getCollector();
            if (c != null)
                c.commit();
            try {
                p.tx.commit();
            } catch (QueueTransactionClosedException e) {
                // ignore
                // Happens when temp queues have been deleted meanwhile
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commitWithoutGlobalLock, t=" + p.tx + ", QueueTransactionClosedException, remove");
                iter.remove();
                p.tx.unlockQueue(false);
            }
        }
        startTransactions();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/commit done");
    }

    void commit() throws Exception {
        if (ctx.queueManager.isUseGlobaLocking())
            commitWithGlobalLock();
        else
            commitWithoutGlobalLock();
    }

    void rollback() throws Exception {
        rollback(true);
    }

    void rollback(boolean start) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/rollback");
        for (Iterator iter = transactions.iterator(); iter.hasNext(); ) {
            Pair p = (Pair) iter.next();
            DestinationCollector c = p.factory.getCollector();
            if (c != null)
                c.abort();
            try {
                p.tx.rollback();
            } catch (QueueTransactionClosedException e) {
                // ignore
                // Happens when temp queues have been deleted meanwhile
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/rollback, t=" + p.tx + ", QueueTransactionClosedException, remove");
                iter.remove();
            }
        }
        if (start)
            startTransactions();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/rollback done");
    }

    void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/close");
        try {
            rollback(false);
        } catch (Exception ignored) {
        }
        transactions.clear();
        transactionFactories.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/close done");
    }

    public String toString() {
        return "TransactionManager";
    }

    private class Pair {
        QueueTransaction tx = null;
        TransactionFactory factory = null;

        private Pair(QueueTransaction tx, TransactionFactory factory) {
            this.tx = tx;
            this.factory = factory;
        }
    }
}

