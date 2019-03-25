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


public class PoolExecutor extends Thread {
    ThreadPoolImpl threadPool;
    long idleTimeout;
    AsyncTask activeTask;
    volatile boolean shouldDie = false;

    PoolExecutor(String name, ThreadGroup threadGroup, ThreadPoolImpl threadPool, long idleTimeout) {
        super(threadGroup, name);
        this.threadPool = threadPool;
        this.idleTimeout = idleTimeout;
    }

    public AsyncTask getActiveTask() {
        return (activeTask);
    }

    void die() {
        shouldDie = true;
        if (activeTask != null)
            activeTask.stop();
    }

    public void run() {
        activeTask = threadPool.getNextTask(this, idleTimeout);
        while (activeTask != null && !shouldDie) {
            if (activeTask.isValid()) {
                try {
                    activeTask.run();
                } catch (Throwable e) {
                    getThreadGroup().uncaughtException(this, e);
                }
            }
            if (!shouldDie)
                activeTask = threadPool.getNextTask(this, idleTimeout);
        }
    }
}

