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

package com.swiftmq.client.thread;

import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.event.FreezeCompletionListener;
import com.swiftmq.tools.collection.RingBuffer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPoolImpl implements ThreadPool {
    private final static int BUCKET_SIZE = 200;

    String poolName = null;
    ThreadGroup threadGroup;
    boolean daemonThreads = false;
    int minThreads;
    int maxThreads;
    int threshold;
    int addThreads;
    int priority;
    long idleTimeout;
    HashSet threadsList = new HashSet();
    int runningCount = 0;
    int idleCount = 0;
    boolean closed = false;
    RingBuffer taskList = null;
    String tname = null;
    int tcount = 0;
    Lock lock = new ReentrantLock();
    Condition taskAvail = null;

    public ThreadPoolImpl(String poolName, boolean daemonThreads, int minThreads, int maxThreads, int threshold, int addThreads, int priority, long idleTimeout) {
        this.daemonThreads = daemonThreads;
        this.poolName = poolName;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.threshold = threshold;
        this.addThreads = addThreads;
        this.priority = priority;
        this.idleTimeout = idleTimeout;
        taskAvail = lock.newCondition();
        tname = "SwiftMQ-" + poolName + "-";
        taskList = new RingBuffer(BUCKET_SIZE);
        // Workaround for applets
        try {
            threadGroup = new ThreadGroup(poolName);
            threadGroup.setMaxPriority(priority);
        } catch (Exception e) {
            threadGroup = null;
        }
        for (int i = 0; i < minThreads; i++)
            createNewThread(-1);
    }

    private void createNewThread(long threadTimeout) {
        PoolExecutor pt = new PoolExecutor(tname + (++tcount), threadGroup, this, threadTimeout);
        pt.setDaemon(daemonThreads);
        runningCount++;
        threadsList.add(pt);
        pt.start();
    }

    public String getPoolName() {
        return poolName;
    }

    public int getNumberRunningThreads() {
        lock.lock();
        try {
            return runningCount;
        } finally {
            lock.unlock();
        }
    }

    public int getNumberIdlingThreads() {
        lock.lock();
        try {
            return idleCount;
        } finally {
            lock.unlock();
        }
    }

    public void dispatchTask(AsyncTask task) {
        lock.lock();
        try {
            if (closed)
                return;
            taskList.add(task);
            int running = runningCount;
            int act = idleCount + running;
            if (act < maxThreads || maxThreads == -1) {
                if (idleCount == 0 && taskList.getSize() - idleCount >= threshold) {
                    for (int i = 0; i < addThreads; i++)
                        createNewThread(idleTimeout);
                } else if (act == 0) {
                    createNewThread(idleTimeout);
                }
            }
            if (idleCount > 0) {
                taskAvail.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    AsyncTask getNextTask(PoolExecutor pt, long timeout) {
        lock.lock();
        try {
            runningCount--;
            AsyncTask task = null;
            if (taskList.getSize() == 0 && !closed) {
                idleCount++;
                try {
                    if (timeout > 0) {
                        long waitStart = System.currentTimeMillis();
                        do {
                            taskAvail.await(timeout, TimeUnit.MILLISECONDS);
                        }
                        while (taskList.getSize() == 0 && !closed && System.currentTimeMillis() - waitStart < timeout);
                    } else {
                        do {
                            taskAvail.await();
                        } while (taskList.getSize() == 0 && !closed);
                    }
                } catch (InterruptedException e) {
                }
                idleCount--;
            }
            if (closed || taskList.getSize() == 0) {
                threadsList.remove(pt);
                return null;
            }
            runningCount++;
            return (AsyncTask) taskList.remove();
        } finally {
            lock.unlock();
        }
    }

    public void freeze(FreezeCompletionListener listener) {
        // do nothing
    }

    public void unfreeze() {
        // do nothing
    }

    public void stop() {
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            for (Iterator iter = threadsList.iterator(); iter.hasNext(); ) {
                PoolExecutor t = (PoolExecutor) iter.next();
                t.die();
            }
            threadsList.clear();
            taskList.clear();
            taskAvail.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
