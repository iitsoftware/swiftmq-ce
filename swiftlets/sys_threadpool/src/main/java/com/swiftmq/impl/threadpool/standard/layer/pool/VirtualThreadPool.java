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
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualThreadPool implements ThreadPool {
    private final ExecutorService virtualExecutor;
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);

    public VirtualThreadPool() {
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public CompletableFuture<?> execute(Runnable task) {
        Runnable wrappedTask = () -> {
            activeTaskCount.incrementAndGet();
            try {
                task.run();
            } finally {
                activeTaskCount.decrementAndGet();
            }
        };
        return CompletableFuture.runAsync(wrappedTask, virtualExecutor);
    }

    public int getActiveTaskCount() {
        return activeTaskCount.get();
    }

    @Override
    public Executor asExecutor() {
        return virtualExecutor;
    }

    @Override
    public void shutdown() {
        virtualExecutor.shutdown();
        try {
            if (!virtualExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                virtualExecutor.shutdownNow();
                if (!virtualExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("VirtualThreadPool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            virtualExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Method to allow controlled shutdown with a custom timeout
    public void shutdown(long timeout, TimeUnit unit) {
        virtualExecutor.shutdown();
        try {
            if (!virtualExecutor.awaitTermination(timeout, unit)) {
                virtualExecutor.shutdownNow();
                if (!virtualExecutor.awaitTermination(timeout, unit)) {
                    System.err.println("VirtualThreadPool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            virtualExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
