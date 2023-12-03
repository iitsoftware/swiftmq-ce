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

package com.swiftmq.impl.store.standard.transaction;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.log.CheckPointFinishedListener;
import com.swiftmq.impl.store.standard.log.CheckPointHandler;
import com.swiftmq.impl.store.standard.log.InitiateSyncOperation;
import com.swiftmq.impl.store.standard.log.SyncLogOperation;
import com.swiftmq.swiftlet.SwiftletManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager implements CheckPointHandler {
    static final InitiateSyncOperation init = new InitiateSyncOperation();
    static final SyncLogOperation sync = new SyncLogOperation();
    StoreContext ctx;
    final AtomicLong txidCount = new AtomicLong();
    final AtomicInteger activeTransactions = new AtomicInteger();
    final AtomicBoolean checkPointInProgress = new AtomicBoolean(false);
    List<CheckPointFinishedListener> finishedListeners = null;
    Lock lock = new ReentrantLock();
    Condition checkpointFinished = null;

    public TransactionManager(StoreContext ctx) {
        this.ctx = ctx;
        checkpointFinished = lock.newCondition();
    }

    public long getTxidCount() {
        return txidCount.get();
    }

    public int getActiveTransactions() {
        return activeTransactions.get();
    }

    public boolean isCheckPointInProgress() {
        return checkPointInProgress.get();
    }

    private void waitForCheckPoint() {
        do {
            checkpointFinished.awaitUninterruptibly();
        } while (checkPointInProgress.get());
    }

    public void lockForCheckPoint() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/lockForCheckPoint...");
            checkPointInProgress.set(true);
            if (activeTransactions.get() == 0)
                ctx.logManager.enqueue(sync);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/lockForCheckPoint...done.");
        } finally {
            lock.unlock();
        }
    }

    public void performCheckPoint() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/performCheckPoint...");
            try {
                // CacheManager must call the listeners inside flush to ensure synchronized access during checkpoint & shrink/backup
                ctx.cacheManager.flush(finishedListeners);
            } catch (Exception e) {
                // PANIC
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/performCheckPoint, exception occurred=" + e);
                ctx.logSwiftlet.logError("sys$store", toString() + "/performCheckPoint, PANIC! EXITING! Exception occurred=" + e);
                SwiftletManager.getInstance().disableShutdownHook();
                System.exit(-1);
            }
            finishedListeners = null;
            ctx.referenceMap.removeReferencesLessThan(1);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/performCheckPoint...done.");
        } finally {
            lock.unlock();
        }
    }

    public void checkPointDone() {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkPointDone...");
            checkPointInProgress.set(false);
            checkpointFinished.signalAll();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/checkPointDone...done.");
        } finally {
            lock.unlock();
        }
    }

    public void initiateCheckPoint(CheckPointFinishedListener finishedListener) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/initiateCheckPoint, finishedListener=" + finishedListener + "...");
            if (checkPointInProgress.get())
                waitForCheckPoint();
            if (finishedListeners == null) {
                finishedListeners = new ArrayList<>();
                ctx.logManager.enqueue(init);
            }
            finishedListeners.add(finishedListener);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/initiateCheckPoint, finishedListener=" + finishedListener + " done");
        } finally {
            lock.unlock();
        }
    }

    public long createTxId() {
        return createTxId(true);
    }

    public long createTxId(boolean doWait) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createTxId, doWait=" + doWait);
            if (checkPointInProgress.get()) {
                if (doWait || activeTransactions.get() == 0) {
                    do {
                        checkpointFinished.awaitUninterruptibly();
                    } while (checkPointInProgress.get() && (doWait || activeTransactions.get() == 0));
                }
            }
            activeTransactions.getAndIncrement();
            long txId = txidCount.getAndIncrement();
            if (txId == Long.MAX_VALUE)
                txidCount.set(0);
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/createTxId, txId=" + txId);
            return txId;
        } finally {
            lock.unlock();
        }
    }

    public void removeTxId(long txId) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/removeTxId, txId=" + txId);
            activeTransactions.getAndDecrement();
            if (checkPointInProgress.get() && activeTransactions.get() == 0)
                ctx.logManager.enqueue(sync);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$store", toString() + "/removeTxId, txId=" + txId + ", done.");
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        return "TransactionManager, activeTransctions=" + activeTransactions;
    }
}

