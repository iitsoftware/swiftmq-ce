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

package com.swiftmq.impl.threadpool.standard.layer.pool;

import java.util.concurrent.*;

public class PlatformThreadPool implements ThreadPool {
    private final ThreadPoolExecutor executor;

    public PlatformThreadPool() {
        this.executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    }

    public CompletableFuture<?> execute(Runnable task) {
        return CompletableFuture.runAsync(task, executor);
    }

    public Executor asExecutor() {
        return executor;
    }

    public int getActiveThreadCount() {
        return executor.getActiveCount();
    }

    public int getIdleThreadCount() {
        return executor.getPoolSize() - getActiveThreadCount();
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("PlatformThreadPool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Optional: Method to allow controlled shutdown with a custom timeout
    public void shutdown(long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeout, unit)) {
                    System.err.println("PlatformThreadPool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
