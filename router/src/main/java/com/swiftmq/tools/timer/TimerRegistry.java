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

package com.swiftmq.tools.timer;

import java.util.*;

/**
 * The TimerRegistry is a Singleton for managing Timers. The
 * application simply uses the add/removeTimerListeners with
 * a delay time as the key. Internal, the TimerRegistry has
 * for all delay times/time points a specific Timer where the add/remove
 * action take place. Is there is no timer for that requested
 * delay time/time point on addTimerListener then this class will create
 * one.
 *
 * @author IIT GmbH
 * @version 2.0
 */
public class TimerRegistry
{
  Timer timer = new Timer(true);
  Map listeners = new HashMap();

  private TimerRegistry()
  {
  }

  private static class InstanceHolder
  {
    public static TimerRegistry instance = new TimerRegistry();
  }

  public static TimerRegistry Singleton()
  {
    return InstanceHolder.instance;
  }

  private long computeDelay(byte timePoint)
  {
    Calendar cal = Calendar.getInstance();
    switch (timePoint)
    {
      case TimerConstants.EVERY_SECOND:
        cal.add(Calendar.SECOND, 1);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case TimerConstants.EVERY_MINUTE:
        cal.add(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case TimerConstants.EVERY_HOUR:
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case TimerConstants.EVERY_DAY:
        cal.add(Calendar.DATE, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case TimerConstants.EVERY_WEEK:
        int firstDay = cal.getFirstDayOfWeek();
        int actDay = cal.get(Calendar.DAY_OF_WEEK);
        cal.add(Calendar.DATE, 7 - (actDay - firstDay));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
      case TimerConstants.EVERY_MONTH:
        cal.add(Calendar.MONTH, 1);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        break;
    }
    return cal.getTime().getTime() - System.currentTimeMillis();
  }

  private long getTimepointMillis(byte timePoint)
  {
    long ms = 0;
    switch (timePoint)
    {
      case TimerConstants.EVERY_SECOND:
        ms = 1;
        break;
      case TimerConstants.EVERY_MINUTE:
        ms = 60;
        break;
      case TimerConstants.EVERY_HOUR:
        ms = 60 * 60;
        break;
      case TimerConstants.EVERY_DAY:
        ms = 60 * 60 * 24;
        break;
      case TimerConstants.EVERY_WEEK:
        ms = 60 * 60 * 24 * 7;
        break;
      case TimerConstants.EVERY_MONTH:
        ms = 60 * 60 * 24 * 30;
        break;
    }
    return ms * 1000;
  }

  public synchronized void addTimerListener(long delay, TimerListener l)
  {
    DelayExecutor exec = new DelayExecutor(delay, l);
    listeners.put(l, exec);
    timer.schedule(exec, delay, delay);
  }

  public synchronized void addInstantTimerListener(long delay, TimerListener l)
  {
    DelayExecutor exec = new DelayExecutor(delay, l);
    timer.schedule(exec, delay);
  }

  public synchronized void addTimerListener(byte timePoint, TimerListener l)
  {
    TimepointExecutor exec = new TimepointExecutor(timePoint, l);
    listeners.put(l, exec);
    timer.scheduleAtFixedRate(exec, computeDelay(timePoint), getTimepointMillis(timePoint));
  }

  public synchronized void removeTimerListener(long delay, TimerListener l)
  {
    TimerTask exec = (TimerTask) listeners.remove(l);
    if (exec != null)
    {
      exec.cancel();
    }
  }

  public synchronized void removeTimerListener(byte timePoint, TimerListener l)
  {
    TimerTask exec = (TimerTask) listeners.remove(l);
    if (exec != null)
    {
      exec.cancel();
    }
  }

  public synchronized void removeAllTimers()
  {
    timer.cancel();
  }

  private class DelayExecutor extends TimerTask
  {
    TimerListener listener = null;
    long delay = 0;

    public DelayExecutor(long delay, TimerListener listener)
    {
      this.delay = delay;
      this.listener = listener;
    }

    public void run()
    {
      TimerListener l = listener;
      if (l != null)
        l.performTimeAction(new TimerEvent(l, delay));
    }

    public boolean cancel()
    {
      boolean b = super.cancel();
      listener = null;
      return b;
    }
  }

  private class TimepointExecutor extends TimerTask
  {
    TimerListener listener = null;
    byte timepoint = 0;

    public TimepointExecutor(byte timepoint, TimerListener listener)
    {
      this.timepoint = timepoint;
      this.listener = listener;
    }

    public void run()
    {
      TimerListener l = listener;
      if (l != null)
        l.performTimeAction(new TimerEvent(l, timepoint));
    }

    public boolean cancel()
    {
      boolean b = super.cancel();
      listener = null;
      return b;
    }
  }

}
