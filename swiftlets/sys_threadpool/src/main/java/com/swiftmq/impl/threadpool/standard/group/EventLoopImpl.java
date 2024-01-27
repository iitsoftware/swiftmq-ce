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
import com.swiftmq.impl.threadpool.standard.group.pool.VirtualThreadRunner;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    public static void main(String[] args) throws InterruptedException {
        MyEventProcessor eventProcessor = new MyEventProcessor(5000);
        EventLoopImpl eventLoop = new EventLoopImpl(null, "1", true, eventProcessor, new VirtualThreadRunner());

        ExecutorService eventSubmitter = Executors.newFixedThreadPool(1);
        System.out.println("Starting event submission...");

        for (int i = 0; i < 5000; i++) {
            int finalI = i;
            eventSubmitter.submit(() -> {
                try {
                    Thread.sleep((long) (10)); // Random delay
                    System.out.println("Submitting event: " + finalI);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                eventLoop.submit(finalI);
            });
        }

        Thread freezeUnfreezeThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    System.out.println("Freezing event loop...");
                    Thread.sleep((long) (1000));
                    eventLoop.freeze();
                    System.out.println("Event loop frozen.");

                    System.out.println("Unfreezing event loop...");
                    Thread.sleep((long) (5000));
                    eventLoop.unfreeze();
                    System.out.println("Event loop unfrozen.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        freezeUnfreezeThread.start();
        Semaphore sem = new Semaphore();
        sem.waitHere(5 * 60000);
        eventSubmitter.shutdown();
        eventSubmitter.awaitTermination(5, TimeUnit.MINUTES);

        System.out.println("Closing event loop...");
        eventLoop.close();
        System.out.println("Event loop closed.");

        freezeUnfreezeThread.interrupt();
        freezeUnfreezeThread.join();

        eventProcessor.checkResults();
        System.out.println("Event processing completed.");
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
//                System.out.println("Wait unfrozen");
                unfrozen.await();
//                System.out.println("unfrozen");
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
//            System.out.println("Thread terminates");
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.threadpoolSwiftlet.getName(), id + "/terminating");
        });
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
        return "EventLoopImpl " +
                "id='" + id + '\'' +
                ", bulkMode=" + bulkMode;
    }

    private void processEvents() {
        boolean shouldProcess = false;
        try {
            queueLock.lock();
            try {
                while (eventQueue.isEmpty() && !shouldTerminate.get() && !isFrozen.get()) {
                    notEmpty.await();
                }
                if (!shouldTerminate.get()) {
                    shouldProcess = true;
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

            // Move event processing outside of the queueLock block
            if (shouldProcess) {
                checkFrozenAndWaitUnfrozen(); // This will handle the freeze logic
                eventProcessor.process(events);
                events.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //Restore the interrupted status
        }
    }

    static class MyEventProcessor implements EventProcessor {
        private final Set<Integer> receivedEvents = new HashSet<>();
        private final int totalEvents;
        private int lastEvent = -1;

        public MyEventProcessor(int totalEvents) {
            this.totalEvents = totalEvents;
        }

        @Override
        public void process(List<Object> events) {
            for (Object event : events) {
                int currentEvent = (Integer) event;
                System.out.println("Process: " + currentEvent);
                if (currentEvent <= lastEvent || receivedEvents.contains(currentEvent)) {
                    System.out.println("Duplicate or out-of-order event detected");
                }
                receivedEvents.add(currentEvent);
                lastEvent = currentEvent;
            }
        }

        public void checkResults() {
            if (receivedEvents.size() != totalEvents) {
                throw new RuntimeException("Mismatch in number of events. Expected: " + totalEvents + ", Received: " + receivedEvents.size());
            }
            for (int i = 0; i < totalEvents; i++) {
                if (!receivedEvents.contains(i)) {
                    throw new RuntimeException("Missing event: " + i);
                }
            }
            System.out.println("All " + totalEvents + " events processed successfully, with no duplicates or missing events.");
        }
    }
}
