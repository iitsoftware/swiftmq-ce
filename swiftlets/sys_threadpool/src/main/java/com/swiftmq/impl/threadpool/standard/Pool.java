/*
 * Copyright 2023 IIT Software GmbH
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

import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.event.FreezeCompletionListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Pool implements ThreadPool {
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    String tracePrefix = null;
    String poolName;
    boolean kernelPool;
    int minThreads;
    int maxThreads;
    int threshold;
    int addThreads;
    int priority;
    long idleTimeout;

    boolean fixedPool;
    boolean stopped = false;
    boolean closed = false;
    FreezeCompletionListener freezeCompletionListener = null;
    boolean freezed = false;
    Lock lock = new ReentrantLock();
    ExecutorService executor = null;
    List<AsyncTask> taskList = new ArrayList<>();

    int runningThreads = 0;

    public Pool(String tracePrefix, String poolName, boolean kernelPool, int minThreads, int maxThreads, int threshold, int addThreads, int priority, long idleTimeout) {
        this.tracePrefix = tracePrefix;
        this.poolName = poolName;
        this.kernelPool = kernelPool;
        this.maxThreads = maxThreads == -1 ? 10 : maxThreads;
        this.fixedPool = maxThreads == 1;
        if (fixedPool || poolName.equals("streams.processor") || poolName.equals("mgmt")) {
            executor = Executors.newFixedThreadPool(maxThreads);
            System.out.println(poolName + ", " + maxThreads + " platform threads ");
        } else {
            executor = Executors.newVirtualThreadPerTaskExecutor();
            System.out.println(poolName + ", virtual threads");
        }
    }

    @Override
    public String getPoolName() {
        return poolName;
    }

    public boolean isKernelPool() {
        lock.lock();
        try {
            return kernelPool;
        } finally {
            lock.unlock();
        }
    }

    public void setKernelPool(boolean b) {
        lock.lock();
        try {
            kernelPool = b;
        } finally {
            lock.unlock();
        }
    }

    public int getMinThreads() {
        lock.lock();
        try {
            return minThreads;
        } finally {
            lock.unlock();
        }
    }

    public void setMinThreads(int minThreads) {
        lock.lock();
        try {
            this.minThreads = minThreads;
        } finally {
            lock.unlock();
        }
    }

    public int getMaxThreads() {
        lock.lock();
        try {
            return maxThreads;
        } finally {
            lock.unlock();
        }
    }

    public void setMaxThreads(int maxThreads) {
        lock.lock();
        try {
            this.maxThreads = maxThreads;
        } finally {
            lock.unlock();
        }
    }

    public int getThreshold() {
        lock.lock();
        try {
            return threshold;
        } finally {
            lock.unlock();
        }
    }

    public void setThreshold(int threshold) {
        lock.lock();
        try {
            this.threshold = threshold;
        } finally {
            lock.unlock();
        }
    }

    public int getAddThreads() {
        lock.lock();
        try {
            return addThreads;
        } finally {
            lock.unlock();
        }
    }

    public void setAddThreads(int addThreads) {
        lock.lock();
        try {
            this.addThreads = addThreads;
        } finally {
            lock.unlock();
        }
    }

    public long getIdleTimeout() {
        lock.lock();
        try {
            return idleTimeout;
        } finally {
            lock.unlock();
        }
    }

    public void setIdleTimeout(long idleTimeout) {
        lock.lock();
        try {
            this.idleTimeout = idleTimeout;
        } finally {
            lock.unlock();
        }
    }

    public int getNumberRunningThreads() {
        lock.lock();
        try {
            return runningThreads;
        } finally {
            lock.unlock();
        }
    }

    public int getNumberIdlingThreads() {
        lock.lock();
        try {
            return 0;
        } finally {
            lock.unlock();
        }
    }

    private void taskComplete() {
        lock.lock();
        try {
            if (closed)
                return;
            runningThreads--;
            if (freezed) {
                if (runningThreads == 0)
                    freezeCompletionListener.freezed(this);
            } else {
                if (fixedPool && taskList.size() > 0 && runningThreads < maxThreads) {
                    execute(taskList.remove(0));
                }
            }

        } finally {
            lock.unlock();
        }
    }

    private void execute(AsyncTask asyncTask) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(asyncTask, executor);
        future.whenComplete((result, throwable) -> taskComplete());
        runningThreads++;
    }

    @Override
    public void dispatchTask(AsyncTask asyncTask) {
        lock.lock();
        try {
            if (stopped || closed)
                return;
            if (freezed || fixedPool && runningThreads == maxThreads)
                taskList.add(asyncTask);
            else
                execute(asyncTask);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void freeze(FreezeCompletionListener freezeCompletionListener) {
        lock.lock();
        try {
            this.freezeCompletionListener = freezeCompletionListener;
            freezed = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unfreeze() {
        lock.lock();
        try {
            this.freezeCompletionListener = null;
            freezed = false;
            taskList.forEach(this::execute);
            taskList.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            this.stopped = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            this.closed = true;
        } finally {
            lock.unlock();
        }
    }
}
