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

package com.swiftmq.impl.net.netty.scheduler;

import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.gc.Recyclable;
import com.swiftmq.tools.gc.Recycler;

import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutor implements Executor {
    private static final String TP_CONNHANDLER = "sys$net.connection.handler";
    SwiftletContext ctx;
    Recycler recycler;
    Lock lock = new ReentrantLock();
    ThreadPool threadPool;
    int numberThreads;

    public TaskExecutor(SwiftletContext ctx) {
        this.ctx = ctx;
        numberThreads = (int)ctx.root.getProperty("number-selector-tasks").getValue();
        recycler = new Recycler(numberThreads) {
            @Override
            protected Recyclable createRecyclable() {
                return new Runner();
            }
        };
        threadPool = ctx.threadpoolSwiftlet.getPool(TP_CONNHANDLER);
    }

    public int getNumberThreads() {
        return numberThreads;
    }

    @Override
    public void execute(Runnable task) {
        lock.lock();
        try {
            Runner runner = (Runner)recycler.checkOut();
            runner.setTask(task);
            threadPool.dispatchTask(runner);
        } finally {
            lock.unlock();
        }
    }

    private void checkIn(Runner runner) {
        lock.lock();
        try {
            recycler.checkIn(runner);
        } finally {
            lock.unlock();
        }

    }

    private class Runner implements AsyncTask, Recyclable {
        boolean stopped = false;
        int recycleIndex = -1;
        Runnable task;

        public void setTask(Runnable task) {
            this.task = task;
        }

        @Override
        public boolean isValid() {
            return !stopped;
        }

        @Override
        public String getDispatchToken() {
            return TP_CONNHANDLER;
        }

        @Override
        public String getDescription() {
            return "Netty Runner";
        }

        @Override
        public void run() {
            task.run();
            checkIn(this);
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void setRecycleIndex(int recycleIndex) {
            this.recycleIndex = recycleIndex;
        }

        @Override
        public int getRecycleIndex() {
            return recycleIndex;
        }

        @Override
        public void reset() {
            stopped = false;
        }
    }
}
