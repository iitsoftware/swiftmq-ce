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

package com.swiftmq.impl.threadpool.standard.layer;

import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class EventLoopImpl implements EventLoop {
    private final BlockingQueue<Object> eventQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean isFrozen = new AtomicBoolean(false);
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicInteger activeRuns = new AtomicInteger(0);
    private final ReentrantLock freezeLock = new ReentrantLock();
    private final Condition freezeCondition = freezeLock.newCondition();
    private Thread thread;
    private final ThreadFactory threadFactory;
    private CloseListener closeListener = null;
    private final boolean bulkMode;
    private final EventProcessor eventProcessor;
    private final List<Object> events = new ArrayList<>();
    private CompletableFuture<Void> freezeFuture = null;

    public EventLoopImpl(boolean bulkMode, EventProcessor eventProcessor, ThreadFactory threadFactory) {
        this.bulkMode = bulkMode;
        this.eventProcessor = eventProcessor;
        this.threadFactory = threadFactory;
        initializeThread();
    }

    private void initializeThread() {
        thread = threadFactory.newThread(() -> {
            while (!isClosing.get()) {
                try {
                    Object event = eventQueue.take();
                    events.add(event);
                    if (bulkMode) {
                        eventQueue.drainTo(events);
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

    public void setCloseListener(CloseListener closeListener) {
        this.closeListener = closeListener;
    }

    public Future<Void> freeze() {
        freezeLock.lock();
        try {
            isFrozen.set(true);
            if (activeRuns.get() == 0) {
                // Complete immediately if no active runs
                return CompletableFuture.completedFuture(null);
            }
            freezeFuture = new CompletableFuture<>();
            thread.interrupt(); // Interrupt the thread to exit from eventQueue.take()
            return freezeFuture;
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
    public void submit(Object event) {
        if (!isClosing.get()) {
            eventQueue.add(event);
        }
    }

    public void internalClose() {
        isClosing.set(true);
        thread.interrupt();
    }

    @Override
    public void close() {
        internalClose();
        if (closeListener != null) {
            closeListener.onClose(this);
        }
    }
}
