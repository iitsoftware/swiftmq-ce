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

package com.swiftmq.impl.timer.standard;

import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.timer.event.SystemTimeChangeListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ConcurrentList;
import org.magicwerk.brownies.collections.GapList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class Dispatcher implements Runnable {
    static final long MAX_DELAY_TIME_CHANGE = 1000;
    SwiftletContext ctx;
    List<TimeTask> taskQueue = new GapList<>();
    Map<TimerListener, TimeTask> timerListeners = new ConcurrentHashMap<>();
    List<SystemTimeChangeListener> sysTimeChangeListeners = new ConcurrentList<>(new ArrayList<>());
    ThreadPool taskPool = null;
    final AtomicLong minDelay = new AtomicLong();
    final AtomicLong maxDelay = new AtomicLong();
    final AtomicLong timeChangeThreshold = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    final AtomicBoolean valid = new AtomicBoolean(true);

    public Dispatcher(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void stop() {
        valid.set(false);
    }

    public void run() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$timer", "Dispatcher, started.");
        long prevTime = System.currentTimeMillis();
        long waitTime = -1;
        while (true) {
            lock.lock();
            try {
                if (taskQueue.isEmpty()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer", "Dispatcher, taskQueue.size() == 0, waiting...");
                    waitTime = -1;
                    notEmpty.await();
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer", "Dispatcher, taskQueue.size() == 0, wake up...");
                    if (!valid.get()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer", "Dispatcher, stop");
                        return;
                    }
                } else {
                    if (waitTime > 0) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer", "Dispatcher, waitTime=" + waitTime + ", waiting...");
                        notEmpty.await(waitTime, TimeUnit.MILLISECONDS);
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer", "Dispatcher, waitTime=" + waitTime + ", wake up...");
                    }
                    if (!valid.get()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer", "Dispatcher, stop");
                        return;
                    }
                    waitTime = -1;
                    long actTime = System.currentTimeMillis();
                    long delta = actTime - prevTime;
                    if (delta < 0 || delta > timeChangeThreshold.get()) {
                        ctx.traceSpace.trace("sys$timer", "Dispatcher, prevTime=" + prevTime + ", actTime=" + actTime + ", delta=" + delta);
                        reorder(delta);
                    }
                    prevTime = actTime;
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer", "Dispatcher, start dispatching, taskQueue.size()=" + taskQueue.size() + ", actTime=" + actTime);
                    for (Iterator<TimeTask> iter = taskQueue.iterator(); iter.hasNext(); ) {
                        TimeTask task = iter.next();
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer", "Dispatcher, dispatching, nextTask=" + task);
                        if (!task.valid.get()) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace("sys$timer", "Dispatcher, dispatching, nextTask=" + task + ", invalid, removing from queue");
                            iter.remove();
                        } else if (task.time.get() <= actTime) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace("sys$timer", "Dispatcher, dispatching, nextTask=" + task);
                            ctx.threadpoolSwiftlet.runAsyncVirtual(task);
                            iter.remove();
                        } else {
                            if (task.base.get() > actTime) {
                                task.base.set(actTime);
                                task.time.set(task.base.get() + task.delay.get());
                            }
                            waitTime = Math.min(Math.max(task.time.get() - actTime, minDelay.get()), maxDelay.get());
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace("sys$timer", "Dispatcher, dispatching, nextTask=" + task + ", task.time > actTime");
                            break;
                        }
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer", "Dispatcher, stop dispatching, actTime=" + actTime + ", new waitTime=" + waitTime);
                }
            } catch (InterruptedException e) {
                break;  // Leave the loop when interrupted
            } finally {
                lock.unlock();
            }
        }

    }

    void enqueue(TimeTask task) {
        lock.lock();
        try {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "enqueue: " + task);
            if (taskQueue.isEmpty()) {
                taskQueue.add(task);
                notEmpty.signal();
            } else {
                int pos = Collections.binarySearch(taskQueue, task);
                if (pos < 0)
                    pos = -pos - 1;
                taskQueue.add(pos, task);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "enqueued at pos: " + pos);

                // If the task is added as the first one, the wait time has changed and
                // the dispatch needs to wake up for processing
                if (pos == 0)
                    notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }

    }

    private void reorder(long delta) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "reorder, delta=" + delta + " ...");
        ctx.logSwiftlet.logInformation("sys$timer", "System time has changed (delta=" + delta + "), reordering timer task queue");
        List<TimeTask> backup = new GapList<>();
        backup.addAll(taskQueue);
        taskQueue.clear();
        for (TimeTask t : backup) {
            if (!t.doNotApplySystemTimeChanges) {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "reorder, before, t=" + t);
                t.recalc(delta);
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "reorder, after, t=" + t);
            }
            enqueue(t);
        }
        for (SystemTimeChangeListener sysTimeChangeListener : sysTimeChangeListeners) {
            try {
                sysTimeChangeListener.systemTimeChangeDetected(delta);
            } catch (Exception ignored) {
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$timer", "reorder, delta=" + delta + " done");
    }

    void addTimerListener(long delay, ThreadPool threadPool, TimerListener listener, boolean instant, boolean doNotApplySystemTimeChanges) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$timer", "addTimerListener, delay=" + delay + ", listener=" + listener + ", instant=" + instant + ", doNotApplySystemTimeChanges=" + doNotApplySystemTimeChanges);
        TimeTask task = new TimeTask(listener);
        if (!instant)
            timerListeners.put(listener, task);
        task.instant = instant;
        task.doNotApplySystemTimeChanges = doNotApplySystemTimeChanges;
        task.delay.set(delay);
        task.base.set(System.currentTimeMillis());
        task.time.set(task.base.get() + delay);
        enqueue(task);
    }

    TimeTask removeTimerListener(TimerListener listener) {
        TimeTask task = timerListeners.remove(listener);
        if (task != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$timer", "removeTimerListener, listener=" + listener + ", found, task=" + task);
            task.valid.set(false);
            task.listener = null;
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$timer", "removeTimerListener, listener=" + listener + ", not found, checking task queue ...");
            removeListenerTasks(listener);
        }
        return task;
    }

    void removeListenerTasks(TimerListener listener) {
        lock.lock();
        try {
            for (Iterator<TimeTask> iter = taskQueue.iterator(); iter.hasNext(); ) {
                TimeTask t = iter.next();
                if (t.listener == listener) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer", "removeTimerListener, listener=" + listener + ", found in task queue, t=" + t);
                    t.valid.set(false);
                    t.listener = null;
                    iter.remove();
                    break;
                }
            }
        } finally {
            lock.unlock();
        }

    }

    void addSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        sysTimeChangeListeners.add(systemTimeChangeListener);
    }

    void removeSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        sysTimeChangeListeners.remove(systemTimeChangeListener);
    }

    void setMinDelay(long minDelay) {
        this.minDelay.set(minDelay);
    }

    void setMaxDelay(long maxDelay) {
        this.maxDelay.set(maxDelay);
        timeChangeThreshold.set(this.maxDelay.get() + MAX_DELAY_TIME_CHANGE);
    }

    void close() {
        if (valid.getAndSet(false))
            return;
        ;
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private class TimeTask implements Runnable, Comparable<TimeTask> {
        final AtomicLong base = new AtomicLong();
        final AtomicLong time = new AtomicLong();
        final AtomicLong delay = new AtomicLong();
        TimerListener listener;
        final AtomicBoolean valid = new AtomicBoolean(true);
        boolean instant = false;
        boolean doNotApplySystemTimeChanges = false;

        TimeTask(TimerListener listener) {
            this.listener = listener;
        }

        public void recalc(long delta) {
            base.addAndGet(delta);
            time.addAndGet(delta);
        }

        public int compareTo(TimeTask thatTask) {
            if (time.get() < thatTask.time.get())
                return -1;
            else if (time.get() == thatTask.time.get())
                return 0;
            return 1;
        }

        public void run() {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$timer/" + this, "performTimeAction()");
            if (listener != null) {
                try {
                    listener.performTimeAction();
                    if (valid.get() && !instant) {
                        base.set(System.currentTimeMillis());
                        time.set(base.get() + delay.get());
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace("sys$timer/" + this, "valid && !instant, enqueue");
                        enqueue(this);
                    } else if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer/" + this, "invalid, stop");
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$timer/" + this, "exception: " + e);
                }
            }
        }

        public String toString() {
            String b = "[TimeTask, base=" + base.get() +
                    ", time=" +
                    time.get() +
                    ", valid=" +
                    valid.get() +
                    ", doNotApplySystemTimeChanges=" +
                    doNotApplySystemTimeChanges +
                    ", instant=" +
                    instant +
                    ", delay=" +
                    delay.get() +
                    ", listener=" +
                    listener +
                    "]";
            return b;
        }
    }
}
