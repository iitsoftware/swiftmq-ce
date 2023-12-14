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

package com.swiftmq.impl.threadpool.standard;

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.event.FreezeCompletionListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.collection.RingBuffer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PoolDispatcher implements ThreadPool {
    private final static int BUCKET_SIZE = 200;

    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    String tracePrefix = null;

    String poolName;
    ThreadGroup threadGroup;
    final AtomicBoolean kernelPool = new AtomicBoolean(false);
    final AtomicInteger minThreads = new AtomicInteger();
    final AtomicInteger maxThreads = new AtomicInteger();
    final AtomicInteger threshold = new AtomicInteger();
    final AtomicInteger addThreads = new AtomicInteger();
    final AtomicInteger priority = new AtomicInteger();
    final AtomicLong idleTimeout = new AtomicLong();
    Set<PoolThread> threadSet = ConcurrentHashMap.newKeySet();
    final AtomicInteger runningCount = new AtomicInteger();
    final AtomicInteger idleCount = new AtomicInteger();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    final AtomicBoolean closed = new AtomicBoolean(false);
    RingBuffer taskList = null;
    FreezeCompletionListener freezeCompletionListener = null;
    final AtomicBoolean freezed = new AtomicBoolean(false);
    final AtomicInteger tcount = new AtomicInteger();
    String tname = null;
    Lock lock = new ReentrantLock();
    Condition taskAvail = null;

    PoolDispatcher(String tracePrefix, String poolName, boolean kernelPool, int minThreads, int maxThreads, int threshold, int addThreads, int priority, long idleTimeout) {
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        this.tracePrefix = tracePrefix + "/" + poolName;
        if (traceSpace.enabled) traceSpace.trace(this.tracePrefix, "initializing");
        taskAvail = lock.newCondition();

        this.kernelPool.set(kernelPool);
        this.poolName = poolName;
        this.minThreads.set(minThreads);
        this.maxThreads.set(maxThreads);
        this.threshold.set(threshold);
        this.addThreads.set(addThreads);
        this.priority.set(priority);
        this.idleTimeout.set(idleTimeout);
        tname = "SwiftMQ-" + poolName + "-";
        threadGroup = new ThreadGroup(poolName);
        threadGroup.setMaxPriority(priority);
        taskList = new RingBuffer(BUCKET_SIZE);
        for (int i = 0; i < minThreads; i++)
            createNewThread(-1);
    }

    private void createNewThread(long threadTimeout) {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "createNewThread, threadTimeout=" + threadTimeout);
        PoolThread pt = new PoolThread(tname + tcount.incrementAndGet(), threadGroup, this, threadTimeout);
        runningCount.getAndIncrement();
        threadSet.add(pt);
        pt.start();
    }

    public String getPoolName() {
        return poolName;
    }

    public void setKernelPool(boolean b) {
        kernelPool.set(b);
    }

    public boolean isKernelPool() {
        return kernelPool.get();
    }

    public int getMinThreads() {
        return minThreads.get();
    }

    public void setMinThreads(int minThreads) {
        this.minThreads.set(minThreads);
    }

    public int getMaxThreads() {
        return maxThreads.get();
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads.set(maxThreads);
    }

    public int getThreshold() {
        return threshold.get();
    }

    public void setThreshold(int threshold) {
        this.threshold.set(threshold);
    }

    public int getAddThreads() {
        return addThreads.get();
    }

    public void setAddThreads(int addThreads) {
        this.addThreads.set(addThreads);
    }

    public long getIdleTimeout() {
        return idleTimeout.get();
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout.set(idleTimeout);
    }

    public int getNumberRunningThreads() {
        return runningCount.get();
    }

    public int getNumberIdlingThreads() {
        return idleCount.get();
    }

    public void dispatchTask(AsyncTask task) {
        lock.lock();
        try {
            if (closed.get() || stopped.get())
                return;
            if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "dispatchTask, dispatchToken=" + task.getDispatchToken() +
                        ", description=" + task.getDescription());
            taskList.add(task);
            if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "dispatchTask, maxThreads=" + maxThreads + ", size=" + taskList.getSize() + ", idle=" + idleCount + ", running=" + runningCount);
            if (!freezed.get()) {
                int running = runningCount.get();
                int act = idleCount.get() + running;
                if (act < maxThreads.get() || maxThreads.get() == -1) {
                    if (idleCount.get() == 0 && taskList.getSize() - idleCount.get() >= threshold.get()) {
                        if (traceSpace.enabled)
                            traceSpace.trace(tracePrefix, "dispatchTask, threshold of " + threshold + " reached, starting " + addThreads + " additional threads...");
                        for (int i = 0; i < addThreads.get(); i++)
                            createNewThread(idleTimeout.get());
                    } else if (act == 0) {
                        if (traceSpace.enabled)
                            traceSpace.trace(tracePrefix, "dispatchTask, no threads running, start up 1 thread...");
                        createNewThread(idleTimeout.get());
                    }
                }
                if (idleCount.get() > 0) {
                    if (traceSpace.enabled)
                        traceSpace.trace(tracePrefix, "dispatchTask, idle=" + idleCount + ", notify...");
                    taskAvail.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    AsyncTask getNextTask(PoolThread pt, long timeout, AsyncTask oldTask) {
        lock.lock();
        try {
            if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", entering...");
            runningCount.getAndDecrement();
            if (stopped.get())
                return null;
            if (maxThreads.get() > 0 && maxThreads.get() <= idleCount.get() + runningCount.get()) {
                if (traceSpace.enabled)
                    traceSpace.trace(tracePrefix, "getNextTask, maxThreads=" + maxThreads + ", idleCount=" + idleCount + ", runningCount=" + runningCount + ", too much threads, this one dies...");
                if (idleCount.get() > 0) {
                    if (traceSpace.enabled)
                        traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", notify...");
                    taskAvail.signal();
                }
                return null;
            }
            if (freezed.get()) {
                threadSet.remove(pt);
                if (threadSet.isEmpty() && freezeCompletionListener != null) {
                    if (traceSpace.enabled)
                        traceSpace.trace(tracePrefix, "getNextTask, freezed, calling freezeCompletionListener");
                    freezeCompletionListener.freezed(this);
                    freezeCompletionListener = null;
                }
                if (traceSpace.enabled) traceSpace.trace(tracePrefix, "getNextTask, freezed, returning null");
                return null;
            }
            AsyncTask task = null;
            if (taskList.getSize() == 0 && !closed.get()) {
                idleCount.getAndIncrement();
                try {
                    if (timeout > 0) {
                        if (traceSpace.enabled)
                            traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wait(timeout)");
                        long waitStart = System.currentTimeMillis();
                        do {
                            taskAvail.await(timeout, TimeUnit.MILLISECONDS);
                        }
                        while (!freezed.get() && taskList.getSize() == 0 && !closed.get() && System.currentTimeMillis() - waitStart < timeout);
                        if (traceSpace.enabled)
                            traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wake up, size=" + taskList.getSize());
                    } else {
                        do {
                            if (traceSpace.enabled)
                                traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wait()");
                            taskAvail.awaitUninterruptibly();
                            if (traceSpace.enabled)
                                traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", wake up, size=" + taskList.getSize());
                        } while (!freezed.get() && taskList.getSize() == 0 && !closed.get());
                    }
                } catch (InterruptedException ignored) {
                }
                idleCount.getAndDecrement();
            }
            if (freezed.get()) {
                threadSet.remove(pt);
                if (threadSet.isEmpty() && freezeCompletionListener != null) {
                    if (traceSpace.enabled)
                        traceSpace.trace(tracePrefix, "getNextTask, freezed, calling freezeCompletionListener");
                    freezeCompletionListener.freezed(this);
                    freezeCompletionListener = null;
                }
                if (traceSpace.enabled) traceSpace.trace(tracePrefix, "getNextTask, freezed, returning null");
                return null;
            }
            if (closed.get() || taskList.getSize() == 0) {
                if (traceSpace.enabled)
                    traceSpace.trace(tracePrefix, "getNextTask, idle=" + idleCount + ", timeout=" + timeout + ", returns null (closed=" + closed + ", size=" + taskList.getSize() + ")");
                threadSet.remove(pt);
                return null;
            }
            task = (AsyncTask) taskList.remove();
            if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "getNextTask, size=" + taskList.getSize() + ", idle=" + idleCount + ", timeout=" + timeout + ", returns: dispatchToken=" + task.getDispatchToken() +
                        ", description=" + task.getDescription());
            runningCount.getAndIncrement();
            return task;
        } finally {
            lock.unlock();
        }
    }

    public void freeze(FreezeCompletionListener freezeCompletionListener) {
        lock.lock();
        try {
            if (traceSpace.enabled)
                traceSpace.trace(tracePrefix, "freeze, freezeCompletionListener=" + freezeCompletionListener);
            this.freezeCompletionListener = freezeCompletionListener;
            freezed.set(true);
            if (threadSet.isEmpty())
                createNewThread(idleTimeout.get());
            else
                taskAvail.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void unfreeze() {
        lock.lock();
        try {
            if (traceSpace.enabled) traceSpace.trace(tracePrefix, "unfreeze");
            freezed.set(false);
            if (minThreads.get() > 0) {
                for (int i = 0; i < minThreads.get(); i++)
                    createNewThread(-1);
            } else if (taskList.getSize() > 0)
                createNewThread(idleTimeout.get());
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        stopped.set(true);
    }

    public void close() {
        lock.lock();
        try {
            if (traceSpace.enabled) traceSpace.trace(tracePrefix, "close, idle=" + idleCount);
            closed.set(true);
            for (PoolThread t : threadSet) {
                t.die();
            }
            threadSet.clear();
            taskList.clear();
            taskAvail.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

