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

package com.swiftmq.impl.timer.standard;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.SystemTimeChangeListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.collection.ConcurrentList;
import org.magicwerk.brownies.collections.GapList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimerSwiftletImpl extends TimerSwiftlet {
    static final String TP_DISPATCHER = "sys$timer.dispatcher";
    static final String TP_TASK = "sys$timer.task";
    static final long MAX_DELAY_TIME_CHANGE = 1000;

    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    ThreadpoolSwiftlet threadpoolSwiftlet = null;
    List<TimeTask> taskQueue = new GapList<>();
    Map<TimerListener, TimeTask> timerListeners = new ConcurrentHashMap<>();
    List<SystemTimeChangeListener> sysTimeChangeListeners = new ConcurrentList<>(new ArrayList<>());
    ThreadPool taskPool = null;
    Dispatcher dispatcher = null;
    long minDelay = 0;
    long maxDelay = 0;
    final AtomicLong timeChangeThreshold = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private void enqueue(TimeTask task) {
        lock.lock();
        try {
            if (traceSpace.enabled) traceSpace.trace(getName(), "enqueue: " + task);
            if (taskQueue.isEmpty()) {
                taskQueue.add(task);
                notEmpty.signal();
            } else {
                int pos = Collections.binarySearch(taskQueue, task);
                if (pos < 0)
                    pos = -pos - 1;
                taskQueue.add(pos, task);
                if (traceSpace.enabled) traceSpace.trace(getName(), "enqueued at pos: " + pos);

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
        if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, delta=" + delta + " ...");
        logSwiftlet.logInformation(getName(), "System time has changed (delta=" + delta + "), reordering timer task queue");
        List<TimeTask> backup = new GapList<>();
        backup.addAll(taskQueue);
        taskQueue.clear();
        for (TimeTask t : backup) {
            if (!t.doNotApplySystemTimeChanges) {
                if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, before, t=" + t);
                t.recalc(delta);
                if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, after, t=" + t);
            }
            enqueue(t);
        }
        for (SystemTimeChangeListener sysTimeChangeListener : sysTimeChangeListeners) {
            try {
                sysTimeChangeListener.systemTimeChangeDetected(delta);
            } catch (Exception ignored) {
            }
        }
        if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, delta=" + delta + " done");
    }

    private void _addTimerListener(long delay, ThreadPool threadPool, TimerListener listener, boolean instant, boolean doNotApplySystemTimeChanges) {
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "addTimerListener, delay=" + delay + ", listener=" + listener + ", instant=" + instant + ", doNotApplySystemTimeChanges=" + doNotApplySystemTimeChanges);
        TimeTask task = new TimeTask(listener, threadPool);
        if (!instant)
            timerListeners.put(listener, task);
        task.instant = instant;
        task.doNotApplySystemTimeChanges = doNotApplySystemTimeChanges;
        task.delay.set(delay);
        task.base.set(System.currentTimeMillis());
        task.time.set(task.base.get() + delay);
        enqueue(task);
    }

    public void addInstantTimerListener(long delay, TimerListener listener) {
        _addTimerListener(delay, null, listener, true, false);
    }

    public void addInstantTimerListener(long delay, ThreadPool threadPool, TimerListener listener) {
        _addTimerListener(delay, threadPool, listener, true, false);
    }

    public void addInstantTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges) {
        _addTimerListener(delay, null, listener, true, doNotApplySystemTimeChanges);
    }

    public void addTimerListener(long delay, TimerListener listener) {
        _addTimerListener(delay, null, listener, false, false);
    }

    public void addTimerListener(long delay, ThreadPool threadPool, TimerListener listener) {
        _addTimerListener(delay, threadPool, listener, false, false);
    }

    public void addTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges) {
        _addTimerListener(delay, null, listener, false, doNotApplySystemTimeChanges);
    }

    public void addTimerListener(long delay, ThreadPool threadPool, TimerListener listener, boolean doNotApplySystemTimeChanges) {
        _addTimerListener(delay, threadPool, listener, false, doNotApplySystemTimeChanges);
    }

    private TimeTask _removeTimerListener(TimerListener listener) {
        return timerListeners.remove(listener);
    }

    public void removeTimerListener(TimerListener listener) {
        if (traceSpace.enabled) traceSpace.trace(getName(), "removeTimerListener, listener=" + listener);
        TimeTask task = _removeTimerListener(listener);
        if (task != null) {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", found, task=" + task);
            task.valid.set(false);
            task.listener = null;
        } else {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", not found, checking task queue ...");
            removeListenerTasks(listener);
        }
    }

    private void removeListenerTasks(TimerListener listener) {
        lock.lock();
        try {
            for (Iterator<TimeTask> iter = taskQueue.iterator(); iter.hasNext(); ) {
                TimeTask t = iter.next();
                if (t.listener == listener) {
                    if (traceSpace.enabled)
                        traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", found in task queue, t=" + t);
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

    public void addSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "addSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
        sysTimeChangeListeners.add(systemTimeChangeListener);
    }

    public void removeSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener) {
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "removeSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
        sysTimeChangeListeners.remove(systemTimeChangeListener);
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        if (traceSpace.enabled) traceSpace.trace(getName(), "startup, ...");
        Property prop = config.getProperty("min-delay");
        minDelay = (Long) prop.getValue();
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                minDelay = (Long) newValue;
            }
        });
        prop = config.getProperty("max-delay");
        maxDelay = (Long) prop.getValue();
        timeChangeThreshold.set(maxDelay + MAX_DELAY_TIME_CHANGE);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                maxDelay = (Long) newValue;
                timeChangeThreshold.set(maxDelay + MAX_DELAY_TIME_CHANGE);
            }
        });
        taskPool = threadpoolSwiftlet.getPool(TP_TASK);
        dispatcher = new Dispatcher();
        threadpoolSwiftlet.dispatchTask(dispatcher);
        if (traceSpace.enabled) traceSpace.trace(getName(), "startup, DONE");
    }

    protected void shutdown()
            throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown, ...");
        dispatcher.valid.set(false);
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }

        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown, DONE");
    }

    private class Dispatcher implements AsyncTask {
        final AtomicBoolean valid = new AtomicBoolean(true);

        public boolean isValid() {
            return valid.get();
        }

        public String getDispatchToken() {
            return TP_DISPATCHER;
        }

        public String getDescription() {
            return "sys$timer/Dispatcher";
        }

        public void stop() {
            valid.set(false);
        }

        public void run() {
            if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, started.");
            long prevTime = System.currentTimeMillis();
            long waitTime = -1;
            while (true) {
                lock.lock();
                try {
                    if (taskQueue.isEmpty()) {
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "Dispatcher, taskQueue.size() == 0, waiting...");
                        waitTime = -1;
                        try {
                            notEmpty.await();
                        } catch (Exception ignored) {
                        }
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "Dispatcher, taskQueue.size() == 0, wake up...");
                        if (!valid.get()) {
                            if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, stop");
                            return;
                        }
                    } else {
                        if (waitTime > 0) {
                            if (traceSpace.enabled)
                                traceSpace.trace(getName(), "Dispatcher, waitTime=" + waitTime + ", waiting...");
                            try {
                                notEmpty.await(waitTime, TimeUnit.MILLISECONDS);
                            } catch (Exception ignored) {
                            }
                            if (traceSpace.enabled)
                                traceSpace.trace(getName(), "Dispatcher, waitTime=" + waitTime + ", wake up...");
                        }
                        if (!valid.get()) {
                            if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, stop");
                            return;
                        }
                        waitTime = -1;
                        long actTime = System.currentTimeMillis();
                        long delta = actTime - prevTime;
                        if (delta < 0 || delta > timeChangeThreshold.get()) {
                            traceSpace.trace(getName(), "Dispatcher, prevTime=" + prevTime + ", actTime=" + actTime + ", delta=" + delta);
                            reorder(delta);
                        }
                        prevTime = actTime;
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "Dispatcher, start dispatching, taskQueue.size()=" + taskQueue.size() + ", actTime=" + actTime);
                        for (Iterator<TimeTask> iter = taskQueue.iterator(); iter.hasNext(); ) {
                            TimeTask task = iter.next();
                            if (traceSpace.enabled)
                                traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task);
                            if (!task.valid.get()) {
                                if (traceSpace.enabled)
                                    traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", invalid, removing from queue");
                                iter.remove();
                            } else if (task.time.get() <= actTime) {
                                if (traceSpace.enabled)
                                    traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", dispatching to thread pool");
                                if (task.pool != null)
                                    task.pool.dispatchTask(task);
                                else
                                    taskPool.dispatchTask(task);
                                iter.remove();
                            } else {
                                if (task.base.get() > actTime) {
                                    task.base.set(actTime);
                                    task.time.set(task.base.get() + task.delay.get());
                                }
                                waitTime = Math.min(Math.max(task.time.get() - actTime, minDelay), maxDelay);
                                if (traceSpace.enabled)
                                    traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", task.time > actTime");
                                break;
                            }
                        }
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "Dispatcher, stop dispatching, actTime=" + actTime + ", new waitTime=" + waitTime);
                    }
                } finally {
                    lock.unlock();
                }
            }

        }
    }

    private class TimeTask implements AsyncTask, Comparable<TimeTask> {
        final AtomicLong base = new AtomicLong();
        final AtomicLong time = new AtomicLong();
        final AtomicLong delay = new AtomicLong();
        TimerListener listener;
        ThreadPool pool = null;
        final AtomicBoolean valid = new AtomicBoolean(true);
        boolean instant = false;
        boolean doNotApplySystemTimeChanges = false;

        TimeTask(TimerListener listener, ThreadPool pool) {
            this.listener = listener;
            this.pool = pool;
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

        public boolean isValid() {
            return valid.get();
        }

        public String getDispatchToken() {
            return TP_TASK;
        }

        public String getDescription() {
            return "TimeTask, time=" + time;
        }

        public void stop() {
            valid.set(false);
        }

        public void run() {
            if (traceSpace.enabled) traceSpace.trace(getName() + "/" + this, "performTimeAction()");
            if (listener != null) {
                try {
                    listener.performTimeAction();
                    if (valid.get() && !instant) {
                        base.set(System.currentTimeMillis());
                        time.set(base.get() + delay.get());
                        if (traceSpace.enabled)
                            traceSpace.trace(getName() + "/" + this, "valid && !instant, enqueue");
                        enqueue(this);
                    } else if (traceSpace.enabled) traceSpace.trace(getName() + "/" + this, "invalid, stop");
                } catch (Exception e) {
                    if (traceSpace.enabled) traceSpace.trace(getName() + "/" + this, "exception: " + e);
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

