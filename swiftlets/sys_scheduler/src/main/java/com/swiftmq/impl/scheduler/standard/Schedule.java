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

package com.swiftmq.impl.scheduler.standard;

import com.swiftmq.impl.scheduler.standard.po.JobStart;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class Schedule implements Serializable
{
  static final boolean respectDST = Boolean.valueOf(System.getProperty("swiftmq.swiftlet.scheduler.respectdst", "true")).booleanValue();
  static final int MAX_DAYS_LOOKUP = 2 * 365;
  static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
  String name = null;
  boolean enabled = false;
  boolean loggingEnabled = false;
  String jobGroup = null;
  String jobName = null;
  String calendar = null;
  String dateFrom = null;
  String dateTo = null;
  long maxRuntime = 0;
  String timeExpression = null;
  Map parameters = new HashMap();

  public Schedule(String name, boolean enabled, boolean loggingEnabled, String jobGroup, String jobName,
                  String calendar, String dateFrom, String dateTo, long maxRuntime, String timeExpression)
  {
    this.name = name;
    this.enabled = enabled;
    this.loggingEnabled = loggingEnabled;
    this.jobGroup = jobGroup;
    this.jobName = jobName;
    this.calendar = calendar;
    this.dateFrom = dateFrom;
    this.dateTo = dateTo;
    this.maxRuntime = maxRuntime;
    this.timeExpression = timeExpression;
  }

  public String getName()
  {
    return name;
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public void setEnabled(boolean enabled)
  {
    this.enabled = enabled;
  }

  public boolean isLoggingEnabled()
  {
    return loggingEnabled;
  }

  public void setLoggingEnabled(boolean loggingEnabled)
  {
    this.loggingEnabled = loggingEnabled;
  }

  public String getCalendar()
  {
    return calendar;
  }

  public void setCalendar(String calendar)
  {
    this.calendar = calendar;
  }

  public String getJobGroup()
  {
    return jobGroup;
  }

  public String getJobName()
  {
    return jobName;
  }

  public String getDateFrom()
  {
    return dateFrom;
  }

  public void setDateFrom(String dateFrom)
  {
    this.dateFrom = dateFrom;
  }

  public String getDateTo()
  {
    return dateTo;
  }

  public void setDateTo(String dateTo)
  {
    this.dateTo = dateTo;
  }

  public long getMaxRuntime()
  {
    return maxRuntime;
  }

  public void setMaxRuntime(long maxRuntime)
  {
    this.maxRuntime = maxRuntime;
  }

  public String getTimeExpression()
  {
    return timeExpression;
  }

  public Map getParameters()
  {
    return parameters;
  }

  public Schedule createCopy() throws Exception
  {
    return (Schedule) Util.deepCopy(this);
  }

  protected int getTimeOfTheDay(Calendar cal)
  {
    return cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
  }

  protected abstract boolean isApplySystemTimeChange();
  
  protected abstract int getNextStart(int lastStartTime, Calendar cal);

  public JobStart getNextJobStart(int lastStartTime, Calendar cal, Map calendars) throws Exception
  {
    // Check whether this Schedule is enabled
    if (!enabled)
      return null;

    Calendar myCal = (Calendar) cal.clone();

    // Determine start date
    Calendar calStart = null;
    if (dateFrom != null)
    {
      calStart = Calendar.getInstance();
      calStart.setTime(fmt.parse(dateFrom));
      calStart.set(Calendar.HOUR_OF_DAY, 0);
      calStart.set(Calendar.MINUTE, 0);
      calStart.set(Calendar.SECOND, 0);
      calStart.set(Calendar.MILLISECOND, 0);
    }

    // Determine the end date
    Calendar calEnd = null;
    if (dateTo != null)
    {
      calEnd = Calendar.getInstance();
      calEnd.setTime(fmt.parse(dateTo));
      calEnd.set(Calendar.HOUR_OF_DAY, 23);
      calEnd.set(Calendar.MINUTE, 59);
      calEnd.set(Calendar.SECOND, 59);
      calEnd.set(Calendar.MILLISECOND, 999);
    }

    // Determine next start time and check against Calendar
    SchedulerCalendar scal = null;
    if (calendar != null)
      scal = (SchedulerCalendar) calendars.get(calendar);
    long startTime = -1;
    int startTimeSec = 0;

    for (int i = 0; i < MAX_DAYS_LOOKUP; i++)
    {
      long calTime = myCal.getTime().getTime();
      if (calEnd != null && calEnd.getTime().getTime() < calTime)
        break;
      if (calStart == null || calStart.getTime().getTime() <= calTime)
      {
        if (scal == null || scal.isValid(myCal.getTime(), calendars))
        {
          int nextTime = getNextStart(lastStartTime, myCal);
          if (nextTime != -1)
          {
            if (respectDST)
            {
              long startOfDay = myCal.getTime().getTime() - (myCal.get(Calendar.HOUR_OF_DAY) * 60 * 60 * 1000) - (myCal.get(Calendar.MINUTE) * 60 * 1000) - (myCal.get(Calendar.SECOND) * 1000) - myCal.get(Calendar.MILLISECOND);
              startTime = startOfDay + nextTime * 1000;
              startTimeSec = nextTime;
            } else
            {
              Calendar c = (Calendar) Calendar.getInstance();
              c.setTime(myCal.getTime());
              c.set(Calendar.HOUR_OF_DAY, 0);
              c.set(Calendar.MINUTE, 0);
              c.set(Calendar.SECOND, 0);
              c.set(Calendar.MILLISECOND, 0);
              c.add(Calendar.SECOND, nextTime);
              startTime = c.getTime().getTime();
              startTimeSec = nextTime;
            }
            break;
          } else
            lastStartTime = -1;
        }
      }
      myCal.add(Calendar.DAY_OF_MONTH, 1);
      if (i == 0)
      {
        myCal.set(Calendar.HOUR_OF_DAY, 0);
        myCal.set(Calendar.MINUTE, 0);
        myCal.set(Calendar.SECOND, 0);
        myCal.set(Calendar.MILLISECOND, 0);
      }
    }
    if (startTime == -1)
      return null;

    // Prepare and return JobStart
    return new JobStart(name, startTime, startTimeSec, maxRuntime);
  }

  public boolean hasCalendarRef(String calendarName, Map calendars)
  {
    boolean ret = false;
    if (calendar != null)
    {
      ret = calendar.equals(calendarName);
      if (!ret)
      {
        SchedulerCalendar cal = (SchedulerCalendar) calendars.get(calendar);
        if (cal != null)
          ret = cal.hasCalendarRef(calendarName, calendars);
      }
    }
    return ret;
  }

  public String toString()
  {
    return "[Schedule, name=" + name +
        ", enabled=" + enabled +
        ", loggingEnabled=" + loggingEnabled +
        ", jobGroup=" + jobGroup +
        ", jobName=" + jobName +
        ", calendar=" + calendar +
        ", dateFrom=" + dateFrom +
        ", dateTo=" + dateTo +
        ", maxRuntime=" + maxRuntime +
        ", timeExpression=" + timeExpression +
        ", parameters=" + parameters +
        "]";
  }
}
