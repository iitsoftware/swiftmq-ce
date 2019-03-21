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
import org.magicwerk.brownies.collections.GapList;

import java.util.*;

public class TimerSwiftletImpl extends TimerSwiftlet
{
  static final String TP_DISPATCHER = "sys$timer.dispatcher";
  static final String TP_TASK = "sys$timer.task";
  static final long MAX_DELAY_TIME_CHANGE_ADDITION = 1000;

  LogSwiftlet logSwiftlet = null;
  TraceSwiftlet traceSwiftlet = null;
  TraceSpace traceSpace = null;
  ThreadpoolSwiftlet threadpoolSwiftlet = null;
  List taskQueue = null;
  Map timerListeners = null;
  List sysTimeChangeListeners = null;
  ThreadPool taskPool = null;
  Dispatcher dispatcher = null;
  long minDelay = 0;
  long maxDelay = 0;
  volatile long timeChangeThreshold = 0;

  private void enqueue(TimeTask task)
  {
    if (traceSpace.enabled) traceSpace.trace(getName(), "enqueue: " + task);
    synchronized (taskQueue)
    {
//      int i = 0;
//      boolean found = false;
//      for (i = 0; i < taskQueue.size(); i++)
//      {
//        TimeTask t = (TimeTask) taskQueue.get(i);
//        if (task.time <= t.time)
//        {
//          taskQueue.add(i, task);
//          found = true;
//          break;
//        }
//      }
//      if (!found)
//        taskQueue.add(task);
//      if (i == 0)
//        taskQueue.notify();
      if (taskQueue.size() == 0)
      {
        taskQueue.add(task);
        taskQueue.notify();
      } else
      {
        int pos = Collections.binarySearch(taskQueue, task);
        if (pos < 0)
          pos = -pos - 1;
        taskQueue.add(pos, task);
        if (traceSpace.enabled) traceSpace.trace(getName(), "enqueued at pos: " + pos);

        // If the task is added as the first one, the wait time has changed and
        // the dispatch needs to wake up for processing
        if (pos == 0)
          taskQueue.notify();
      }
    }
  }

  private void reorder(long delta)
  {
    if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, delta=" + delta + " ...");
    logSwiftlet.logInformation(getName(), "System time has changed (delta=" + delta + "), reordering timer task queue");
    List backup = (List) ((GapList) taskQueue).clone();
    taskQueue.clear();
    for (Iterator iter = backup.iterator(); iter.hasNext();)
    {
      TimeTask t = (TimeTask) iter.next();
      if (!t.doNotApplySystemTimeChanges)
      {
        if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, before, t=" + t);
        t.recalc(delta);
        if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, after, t=" + t);
      }
      enqueue(t);
    }
    for (int i = 0; i < sysTimeChangeListeners.size(); i++)
    {
      SystemTimeChangeListener l = null;
      try
      {
        l = (SystemTimeChangeListener) sysTimeChangeListeners.get(i);
        l.systemTimeChangeDetected(delta);
      } catch (Exception e)
      {
        logSwiftlet.logInformation(getName(), "Exception calling SystemTimeChangeListener '" + l + "': " + e);
        if (traceSpace.enabled)
          traceSpace.trace(getName(), "Exception calling SystemTimeChangeListener '" + l + "': " + e);
      }
    }
    if (traceSpace.enabled) traceSpace.trace(getName(), "reorder, delta=" + delta + " done");
  }

  private synchronized void _addTimerListener(long delay, ThreadPool threadPool, TimerListener listener, boolean instant, boolean doNotApplySystemTimeChanges)
  {
    if (traceSpace.enabled)
      traceSpace.trace(getName(), "addTimerListener, delay=" + delay + ", listener=" + listener + ", instant=" + instant + ", doNotApplySystemTimeChanges=" + doNotApplySystemTimeChanges);
    TimeTask task = new TimeTask(listener, threadPool);
    if (!instant)
      timerListeners.put(listener, task);
    task.instant = instant;
    task.doNotApplySystemTimeChanges = doNotApplySystemTimeChanges;
    task.delay = delay;
    task.base = System.currentTimeMillis();
    task.time = task.base + delay;
    enqueue(task);
  }

  public void addInstantTimerListener(long delay, TimerListener listener)
  {
    _addTimerListener(delay, null, listener, true, false);
  }

  public void addInstantTimerListener(long delay, ThreadPool threadPool, TimerListener listener)
  {
    _addTimerListener(delay, threadPool, listener, true, false);
  }

  public void addInstantTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges)
  {
    _addTimerListener(delay, null, listener, true, doNotApplySystemTimeChanges);
  }

  public void addTimerListener(long delay, TimerListener listener)
  {
    _addTimerListener(delay, null, listener, false, false);
  }

  public void addTimerListener(long delay, ThreadPool threadPool, TimerListener listener)
  {
    _addTimerListener(delay, threadPool, listener, false, false);
  }

  public void addTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges)
  {
    _addTimerListener(delay, null, listener, false, doNotApplySystemTimeChanges);
  }

  public void addTimerListener(long delay, ThreadPool threadPool, TimerListener listener, boolean doNotApplySystemTimeChanges)
  {
    _addTimerListener(delay, threadPool, listener, false, doNotApplySystemTimeChanges);
  }

  private synchronized TimeTask _removeTimerListener(TimerListener listener)
  {
    TimeTask task = (TimeTask) timerListeners.remove(listener);
    return task;
  }

  public void removeTimerListener(TimerListener listener)
  {
    if (traceSpace.enabled) traceSpace.trace(getName(), "removeTimerListener, listener=" + listener);
    TimeTask task = _removeTimerListener(listener);
    if (task != null)
    {
      if (traceSpace.enabled)
        traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", found, task=" + task);
      task.valid = false;
      task.listener = null;
    } else
    {
      if (traceSpace.enabled)
        traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", not found, checking task queue ...");
      synchronized (taskQueue)
      {
        for (Iterator iter = taskQueue.iterator(); iter.hasNext();)
        {
          TimeTask t = (TimeTask) iter.next();
          if (t.listener == listener)
          {
            if (traceSpace.enabled)
              traceSpace.trace(getName(), "removeTimerListener, listener=" + listener + ", found in task queue, t=" + t);
            t.valid = false;
            t.listener = null;
            iter.remove();
            break;
          }
        }
      }
    }
  }

  public synchronized void addSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener)
  {
    if (traceSpace.enabled)
      traceSpace.trace(getName(), "addSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
    sysTimeChangeListeners.add(systemTimeChangeListener);
  }

  public synchronized void removeSystemTimeChangeListener(SystemTimeChangeListener systemTimeChangeListener)
  {
    if (traceSpace.enabled)
      traceSpace.trace(getName(), "removeSystemTimeChangeListener, systemTimeChangeListener=" + systemTimeChangeListener);
    sysTimeChangeListeners.remove(systemTimeChangeListener);
  }

  protected void startup(Configuration config)
      throws SwiftletException
  {
    logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    if (traceSpace.enabled) traceSpace.trace(getName(), "startup, ...");
    Property prop = config.getProperty("min-delay");
    minDelay = ((Long) prop.getValue()).longValue();
    prop.setPropertyChangeListener(new PropertyChangeAdapter(null)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        minDelay = ((Long) newValue).longValue();
      }
    });
    prop = config.getProperty("max-delay");
    maxDelay = ((Long) prop.getValue()).longValue();
    timeChangeThreshold = maxDelay + MAX_DELAY_TIME_CHANGE_ADDITION;
    prop.setPropertyChangeListener(new PropertyChangeAdapter(null)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        maxDelay = ((Long) newValue).longValue();
        timeChangeThreshold = maxDelay + MAX_DELAY_TIME_CHANGE_ADDITION;
      }
    });
    taskPool = threadpoolSwiftlet.getPool(TP_TASK);
    taskQueue = new GapList();
    timerListeners = new HashMap();
    sysTimeChangeListeners = new GapList();
    dispatcher = new Dispatcher();
    threadpoolSwiftlet.dispatchTask(dispatcher);
    if (traceSpace.enabled) traceSpace.trace(getName(), "startup, DONE");
  }

  protected synchronized void shutdown()
      throws SwiftletException
  {
    if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown, ...");
    dispatcher.valid = false;
    synchronized (taskQueue)
    {
      taskQueue.notify();
    }
    if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown, DONE");
  }

  private class Dispatcher implements AsyncTask
  {
    boolean valid = true;

    public boolean isValid()
    {
      return valid;
    }

    public String getDispatchToken()
    {
      return TP_DISPATCHER;
    }

    public String getDescription()
    {
      return "sys$timer/Dispatcher";
    }

    public void stop()
    {
      valid = false;
    }

    public void run()
    {
      if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, started.");
      long prevTime = System.currentTimeMillis();
      long waitTime = -1;
      synchronized (taskQueue)
      {
        for (; ;)
        {
          if (taskQueue.size() == 0)
          {
            if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, taskQueue.size() == 0, waiting...");
            waitTime = -1;
            try
            {
              taskQueue.wait();
            } catch (Exception ignored)
            {
            }
            if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, taskQueue.size() == 0, wake up...");
            if (!valid)
            {
              if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, stop");
              return;
            }
          } else
          {
            if (waitTime > 0)
            {
              if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, waitTime=" + waitTime + ", waiting...");
              try
              {
                taskQueue.wait(waitTime);
              } catch (Exception ignored)
              {
              }
              if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, waitTime=" + waitTime + ", wake up...");
            }
            if (!valid)
            {
              if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, stop");
              return;
            }
            waitTime = -1;
            long actTime = System.currentTimeMillis();
            long delta = actTime - prevTime;
            if (delta < 0 || delta > timeChangeThreshold)
            {
              traceSpace.trace(getName(), "Dispatcher, prevTime=" + prevTime + ", actTime=" + actTime + ", delta=" + delta);
              reorder(delta);
            }
            prevTime = actTime;
            if (traceSpace.enabled)
              traceSpace.trace(getName(), "Dispatcher, start dispatching, taskQueue.size()=" + taskQueue.size() + ", actTime=" + actTime);
            for (Iterator iter = taskQueue.iterator(); iter.hasNext();)
            {
              TimeTask task = (TimeTask) iter.next();
              if (traceSpace.enabled) traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task);
              if (!task.valid)
              {
                if (traceSpace.enabled)
                  traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", invalid, removing from queue");
                iter.remove();
              } else if (task.time <= actTime)
              {
                if (traceSpace.enabled)
                  traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", dispatching to thread pool");
                if (task.pool != null)
                  task.pool.dispatchTask(task);
                else
                  taskPool.dispatchTask(task);
                iter.remove();
              } else
              {
                if (task.base > actTime)
                {
                  task.base = actTime;
                  task.time = task.base + task.delay;
                }
                waitTime = Math.min(Math.max(task.time - actTime, minDelay), maxDelay);
                if (traceSpace.enabled)
                  traceSpace.trace(getName(), "Dispatcher, dispatching, nextTask=" + task + ", task.time > actTime");
                break;
              }
            }
            if (traceSpace.enabled)
              traceSpace.trace(getName(), "Dispatcher, stop dispatching, actTime=" + actTime + ", new waitTime=" + waitTime);
          }
        }
      }
    }
  }

  private class TimeTask implements AsyncTask, Comparable
  {
    long base = 0;
    long time = 0;
    long delay = 0;
    TimerListener listener;
    ThreadPool pool = null;
    boolean valid = true;
    boolean instant = false;
    boolean doNotApplySystemTimeChanges = false;

    TimeTask(TimerListener listener)
    {
      this.listener = listener;
    }

    TimeTask(TimerListener listener, ThreadPool pool)
    {
      this.listener = listener;
      this.pool = pool;
    }

    public synchronized void recalc(long delta)
    {
      base += delta;
      time += delta;
    }

    public int compareTo(Object that)
    {
      TimeTask thatTask = (TimeTask) that;
      if (time < thatTask.time)
        return -1;
      else if (time == thatTask.time)
        return 0;
      return 1;
    }

    public boolean isValid()
    {
      return valid;
    }

    public String getDispatchToken()
    {
      return TP_TASK;
    }

    public String getDescription()
    {
      return "TimeTask, time=" + time;
    }

    public void stop()
    {
      valid = false;
    }

    public void run()
    {
      if (traceSpace.enabled) traceSpace.trace(getName() + "/" + toString(), "performTimeAction()");
      if (listener != null)
      {
        try
        {
          listener.performTimeAction();
          if (valid && !instant)
          {
            base = System.currentTimeMillis();
            time = base + delay;
            if (traceSpace.enabled) traceSpace.trace(getName() + "/" + toString(), "valid && !instant, enqueue");
            enqueue(this);
          } else if (traceSpace.enabled) traceSpace.trace(getName() + "/" + toString(), "invalid, stop");
        } catch (Exception e)
        {
          if (traceSpace.enabled) traceSpace.trace(getName() + "/" + toString(), "exception: " + e);
        }
      }
    }

    public String toString()
    {
      StringBuffer b = new StringBuffer("[TimeTask, base=");
      b.append(base);
      b.append(", time=");
      b.append(time);
      b.append(", valid=");
      b.append(valid);
      b.append(", doNotApplySystemTimeChanges=");
      b.append(doNotApplySystemTimeChanges);
      b.append(", instant=");
      b.append(instant);
      b.append(", delay=");
      b.append(delay);
      b.append(", listener=");
      b.append(listener);
      b.append("]");
      return b.toString();
    }
  }
}

