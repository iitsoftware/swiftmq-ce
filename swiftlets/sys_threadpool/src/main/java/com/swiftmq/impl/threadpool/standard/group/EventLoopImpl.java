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

package com.swiftmq.impl.threadpool.standard.group;

import com.swiftmq.impl.threadpool.standard.SwiftletContext;
import com.swiftmq.impl.threadpool.standard.group.pool.ThreadRunner;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class EventLoopImpl implements EventLoop {
    private SwiftletContext ctx;
    private final List<Object> eventQueue = new LinkedList<>();
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private final AtomicBoolean isFrozen = new AtomicBoolean(false);
    private final ReentrantLock queueLock = new ReentrantLock();
    private final ReentrantLock freezeLock = new ReentrantLock();
    private final Condition notEmpty = queueLock.newCondition();
    private final Condition unfrozen = freezeLock.newCondition();
    private final AtomicReference<CompletableFuture<Void>> freezeFuture = new AtomicReference<>();
    private volatile Thread thread = null;
    private final ThreadRunner threadPool;
    private CloseListener closeListener = null;
    private String id;
    private final boolean bulkMode;
    private final EventProcessor eventProcessor;
    private final List<Object> events = new ArrayList<>();

    public EventLoopImpl(SwiftletContext ctx, String id, boolean bulkMode, EventProcessor eventProcessor, ThreadRunner threadPool) {
        this.ctx = ctx;
        this.id = id;
        this.bulkMode = bulkMode;
        this.eventProcessor = eventProcessor;
        this.threadPool = threadPool;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/created");
        initializeThread();
    }

    public CompletableFuture<Void> freeze() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/freeze");
        isFrozen.set(true);
        freezeFuture.set(new CompletableFuture<>());
        queueLock.lock();
        try {
            notEmpty.signalAll(); // Wake up the thread if it's waiting
        } finally {
            queueLock.unlock();
        }
        return freezeFuture.get();
    }

    public void unfreeze() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/unfreeze");
        freezeLock.lock();
        try {
            isFrozen.set(false);
            unfrozen.signalAll();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/unfreeze done");
        } finally {
            freezeLock.unlock();
        }
    }

    private void checkFrozenAndWaitUnfrozen() {
        freezeLock.lock();
        try {
            while (isFrozen.get()) {
                CompletableFuture<Void> f = freezeFuture.getAndSet(null);
                if (f != null && !f.isDone()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/entering frozen state");
                    f.complete(null);
                }

                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/waiting to be unfrozen");
                unfrozen.await();
            }
        } catch (InterruptedException e) {
            // Handle thread interruption, for example during shutdown
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/interrupted while waiting to be unfrozen");
        } finally {
            freezeLock.unlock();
        }
    }

    private void initializeThread() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/initializeThread");
        threadPool.execute(() -> {
            thread = Thread.currentThread();
            while (!shouldTerminate.get()) {
                processEvents();
            }
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/terminating");
        });
    }

    private void processEvents() {
        try {
            queueLock.lock();
            try {
                while (eventQueue.isEmpty() && !shouldTerminate.get() && !isFrozen.get()) {
                    notEmpty.await();
                    if (isFrozen.get()) {
                        break;
                    }
                }
                if (!shouldTerminate.get() && !eventQueue.isEmpty()) {
                    if (bulkMode) {
                        events.addAll(eventQueue);
                        eventQueue.clear();
                    } else {
                        events.add(eventQueue.removeFirst());
                    }
                }
            } finally {
                queueLock.unlock();
            }

            checkFrozenAndWaitUnfrozen(); // This will handle the freeze logic

            // Move event processing outside the queueLock block
            // empty() may only happen during frozen state
            if (!events.isEmpty()) {
                eventProcessor.process(events);
                events.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //Restore the interrupted status
        }
    }

    @Override
    public void submit(Object event) {
        queueLock.lock();
        try {
            eventQueue.add(event);
            notEmpty.signal();
        } finally {
            queueLock.unlock();
        }
    }

    public void setCloseListener(CloseListener closeListener) {
        this.closeListener = closeListener;
    }

    public void internalClose() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/internalClose  ...");
        shouldTerminate.set(true);
        queueLock.lock();
        try {
            notEmpty.signalAll(); // Wake up the thread if it's waiting
        } finally {
            queueLock.unlock();
        }
        if (thread != null) {
            thread.interrupt(); // Optional: Interrupt the thread to speed up closing
        }
        freezeLock.lock();
        try {
            unfrozen.signalAll(); // Wake up the thread if it's waiting
        } finally {
            freezeLock.unlock();
        }
        if (closeListener != null) {
            closeListener.onClose(this);
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/internalClose done");
    }

    @Override
    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/close  ...");
        internalClose();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/close  done");
    }

    @Override
    public String toString() {
        return "[EventLoopImpl " +
                "id='" + id + '\'' +
                ", bulkMode=" + bulkMode +
                ", processor=" + eventProcessor + "]";
    }

}
