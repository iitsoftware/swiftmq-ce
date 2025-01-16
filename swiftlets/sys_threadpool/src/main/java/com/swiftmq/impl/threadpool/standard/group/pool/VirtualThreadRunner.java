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

package com.swiftmq.impl.threadpool.standard.group.pool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualThreadRunner implements ThreadRunner {
    private final ExecutorService virtualExecutor;
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);
    public VirtualThreadRunner() {
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public CompletableFuture<?> execute(Runnable task) {
        Runnable wrappedTask = () -> {
            // Capture the system class loader
            ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
            ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

            // Set the system class loader for the virtual thread
            Thread.currentThread().setContextClassLoader(systemClassLoader);
            try {
                activeTaskCount.incrementAndGet();
                task.run();
            } finally {
                // Restore the original class loader after the task is done
                Thread.currentThread().setContextClassLoader(originalClassLoader);
                activeTaskCount.decrementAndGet();
            }
        };
        return CompletableFuture.runAsync(wrappedTask, virtualExecutor);
    }

    public int getActiveThreadCount() {
        return activeTaskCount.get();
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
                    System.err.println("VirtualThreadRunner did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            virtualExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
