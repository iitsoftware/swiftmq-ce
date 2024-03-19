/*
 * Copyright 2024 IIT Software GmbH
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

import com.swiftmq.impl.threadpool.standard.SwiftletContext;

import java.util.Deque;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RejectionHandler implements RejectedExecutionHandler {
    private final SwiftletContext ctx;
    private final Deque<Runnable> retryDeque = new ConcurrentLinkedDeque<>();
    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean timerActive = new AtomicBoolean(false);

    public RejectionHandler(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//        ctx.logSwiftlet.logInformation(ctx.threadpoolSwiftlet.getName(), this + "/rejectedExecution, retryDeque.size=" + retryDeque.size());
//        logUsage(executor);
        retryDeque.offer(r); // Rejected tasks are enqueued here
        scheduleDrainQueue(executor);
    }

    private void scheduleDrainQueue(ThreadPoolExecutor executor) {
        if (timerActive.compareAndSet(false, true)) {
//            ctx.logSwiftlet.logInformation(ctx.threadpoolSwiftlet.getName(), this + "/scheduleDrainQueue");
            timer.schedule(() -> drainQueueToExecutor(executor), 1, TimeUnit.SECONDS);
        }
    }

    private void logUsage(ThreadPoolExecutor executor) {
        int poolSize = executor.getPoolSize();
        int active = executor.getActiveCount();
        int idle = poolSize - active;
        ctx.logSwiftlet.logInformation(ctx.threadpoolSwiftlet.getName(), this + "/logUsage, poolSize=" + poolSize + ", active=" + active + ", idle=" + idle);
    }

    private void drainQueueToExecutor(ThreadPoolExecutor executor) {
        Runnable task;
        while ((task = retryDeque.poll()) != null) {
//            ctx.logSwiftlet.logInformation(ctx.threadpoolSwiftlet.getName(), this + "/drainQueueToExecutor, queue.size=" + retryDeque.size());
            executor.execute(task);
        }
        timerActive.set(false); // Set to false when retryDeque is empty
        if (!retryDeque.isEmpty()) {
            scheduleDrainQueue(executor); // Reschedule if there are more tasks
        }
//        logUsage(executor);
    }

    public void stopTimer() {
//        ctx.logSwiftlet.logInformation(ctx.threadpoolSwiftlet.getName(), this + "/stopTimer");
        timer.shutdownNow();
    }

    @Override
    public String toString() {
        return "RejectionHandler";
    }
}
