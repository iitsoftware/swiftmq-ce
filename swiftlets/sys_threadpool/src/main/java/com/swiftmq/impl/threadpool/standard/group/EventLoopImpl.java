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

import com.swiftmq.impl.threadpool.standard.group.pool.ThreadRunner;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.collection.ConcurrentList;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class EventLoopImpl implements EventLoop {
    private final List<Object> eventQueue = new LinkedList<>();
    private final AtomicBoolean isFrozen = new AtomicBoolean(false);
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicInteger activeRuns = new AtomicInteger(0);
    private final ReentrantLock freezeLock = new ReentrantLock();
    private final ReentrantLock queueLock = new ReentrantLock();
    private final Condition notEmpty = queueLock.newCondition();
    private final Condition freezeCondition = freezeLock.newCondition();
    private volatile Thread thread = null;
    private final ThreadRunner threadPool;
    private CloseListener closeListener = null;
    private String id;
    private final boolean bulkMode;
    private final EventProcessor eventProcessor;
    private final List<Object> events = new ArrayList<>();
    private CompletableFuture<Void> freezeFuture = null;
    private final List<CompletableFuture<?>> taskFutures = new ConcurrentList<>(new ArrayList<>());

    public EventLoopImpl(String id, boolean bulkMode, EventProcessor eventProcessor, ThreadRunner threadPool) {
        this.id = id;
        this.bulkMode = bulkMode;
        this.eventProcessor = eventProcessor;
        this.threadPool = threadPool;
        initializeThread();
    }

    private void initializeThread() {
        threadPool.execute(() -> {
            thread = Thread.currentThread(); // store it for interruption
            while (!isClosing.get()) {
                try {
                    queueLock.lock();
                    try {
                        if (eventQueue.isEmpty())
                            notEmpty.await();
                        if (bulkMode) {
                            events.addAll(eventQueue);
                            eventQueue.clear();
                        } else
                            events.add(eventQueue.removeFirst());
                    } finally {
                        queueLock.unlock();
                    }

                    activeRuns.incrementAndGet();
                    try {
                        eventProcessor.process(events);
                    } finally {
                        events.clear();
                        activeRuns.decrementAndGet();
                        signalIfFrozen();
                    }
                } catch (InterruptedException e) {
                    if (isClosing.get()) {
                        // If closing, exit the loop and terminate the thread
                        break;
                    }
                    // If interrupted for other reasons, such as freeze, check the freeze state
                    checkFreeze();
                }
            }
        });
    }


    private void checkFreeze() {
        freezeLock.lock();
        try {
            while (isFrozen.get()) {
                try {
                    freezeCondition.await();
                } catch (InterruptedException e) {
                    // Check if the interruption is due to closing
                    if (isClosing.get()) {
                        break;
                    }
                    // If not closing, continue waiting
                }
            }
        } finally {
            freezeLock.unlock();
        }
    }

    private void signalIfFrozen() {
        freezeLock.lock();
        try {
            if (isFrozen.get() && activeRuns.get() == 0) {
                freezeCondition.signalAll();
                if (freezeFuture != null && !freezeFuture.isDone()) {
                    freezeFuture.complete(null);
                }
            }
        } finally {
            freezeLock.unlock();
        }
    }

    private void waitForAllTasksCompletion() {
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(taskFutures.toArray(new CompletableFuture[0]));
        try {
            allTasks.get();
        } catch (InterruptedException | ExecutionException e) {
            // Handle exceptions, possibly log them or rethrow as appropriate
        }
        taskFutures.clear();
    }

    public void setCloseListener(CloseListener closeListener) {
        this.closeListener = closeListener;
    }

    public CompletableFuture<Void> freeze() {
        freezeLock.lock();
        try {
            isFrozen.set(true);
            if (activeRuns.get() == 0) {
                // Complete immediately if no active runs
                return CompletableFuture.completedFuture(null);
            }
            freezeFuture = new CompletableFuture<Void>();
            thread.interrupt(); // Interrupt the thread to exit from eventQueue.take()
            return (CompletableFuture<Void>) freezeFuture;
        } finally {
            freezeLock.unlock();
        }
    }

    public void unfreeze() {
        freezeLock.lock();
        try {
            isFrozen.set(false);
            freezeFuture = null;
            freezeCondition.signalAll(); // Wake up threads waiting in checkFreeze
        } finally {
            freezeLock.unlock();
        }
    }

    @Override
    public CompletableFuture<?> executeInNewThread(Runnable runnable) {
        CompletableFuture<?> taskFuture = threadPool.execute(runnable);
        taskFutures.add(taskFuture);
        // Remove the future from the list when it completes
        taskFuture.whenComplete((result, throwable) -> taskFutures.remove(taskFuture));
        return taskFuture;
    }

    @Override
    public void submit(Object event) {
        queueLock.lock();
        try {
            if (!isClosing.get()) {
                eventQueue.add(event);
                notEmpty.signal();
            }
        } finally {
            queueLock.unlock();
        }

    }

    public void internalClose() {
        isClosing.set(true);
        queueLock.lock();
        try {
            notEmpty.signal();
        } finally {
            queueLock.unlock();
        }
        thread.interrupt();
        waitForAllTasksCompletion();
    }

    @Override
    public void close() {
        internalClose();
        if (closeListener != null) {
            closeListener.onClose(this);
        }
    }

    @Override
    public String toString() {
        return "EventLoopImpl {" +
                "id='" + id + '\'' +
                ", bulkMode=" + bulkMode +
                '}';
    }
}
