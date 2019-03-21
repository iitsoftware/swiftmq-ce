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

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class SchedulerCalendar implements Serializable
{
  public static int LAST_DAY_OF_MONTH = 32;
  static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
  static Map months = new HashMap();
  static {
    months.put("January",new Integer(Calendar.JANUARY));
    months.put("February",new Integer(Calendar.FEBRUARY));
    months.put("March",new Integer(Calendar.MARCH));
    months.put("April",new Integer(Calendar.APRIL));
    months.put("May",new Integer(Calendar.MAY));
    months.put("June",new Integer(Calendar.JUNE));
    months.put("July",new Integer(Calendar.JULY));
    months.put("August",new Integer(Calendar.AUGUST));
    months.put("September",new Integer(Calendar.SEPTEMBER));
    months.put("October",new Integer(Calendar.OCTOBER));
    months.put("November",new Integer(Calendar.NOVEMBER));
    months.put("December",new Integer(Calendar.DECEMBER));
  }
  String name = null;
  boolean exclude = true;
  String baseCalendarName = null;
  boolean weekDays[] = new boolean[7];
  boolean monthDays[] = new boolean[32];
  Map annualDays = new HashMap();
  Map dateRanges = new HashMap();
  boolean enableWeekDays = false;
  boolean enableMonthDays = false;
  boolean enableMonthDayLast = false;
  boolean enableAnnualDays = false;
  boolean enableDateRanges = false;

  public SchedulerCalendar(String name, boolean exclude, String baseCalendarName,
                           boolean enableWeekDays, boolean enableMonthDays, boolean enableMonthDayLast, boolean enableAnnualDays, boolean enableDateRanges)
  {
    this.name = name;
    this.exclude = exclude;
    this.baseCalendarName = baseCalendarName;
    this.enableWeekDays = enableWeekDays;
    this.enableMonthDays = enableMonthDays;
    this.enableMonthDayLast = enableMonthDayLast;
    this.enableAnnualDays = enableAnnualDays;
    this.enableDateRanges = enableDateRanges;
  }

  public String getName()
  {
    return name;
  }

  public void setExclude(boolean exclude)
  {
    this.exclude = exclude;
  }

  public void setBaseCalendarName(String baseCalendarName)
  {
    this.baseCalendarName = baseCalendarName;
  }

  public void setEnableWeekDays(boolean enableWeekDays)
  {
    this.enableWeekDays = enableWeekDays;
  }

  public void setEnableMonthDays(boolean enableMonthDays)
  {
    this.enableMonthDays = enableMonthDays;
  }

  public void setEnableMonthDayLast(boolean enableMonthDayLast)
  {
    this.enableMonthDayLast = enableMonthDayLast;
  }

  public void setEnableAnnualDays(boolean enableAnnualDays)
  {
    this.enableAnnualDays = enableAnnualDays;
  }

  public void setEnableDateRanges(boolean enableDateRanges)
  {
    this.enableDateRanges = enableDateRanges;
  }

  public void setWeekDay(int day, boolean b)
  {
    weekDays[day-1] = b;
  }

  public void setMonthDay(int day, boolean b)
  {
    monthDays[day-1] = b;
  }

  public void addAnnualDay(String name, int day, String month)
  {
    addAnnualDay(name,day,((Integer)months.get(month)).intValue());
  }

  public void addAnnualDay(String name, int day, int month)
  {
    annualDays.put(name,new AnnualDay(day,month));
  }

  public void removeAnnualDay(String name)
  {
    annualDays.remove(name);
  }

  public void addDateRange(String name, String from, String to)
  {
    dateRanges.put(name,new DateRange(from,to));
  }

  public void removeDateRange(String name)
  {
    dateRanges.remove(name);
  }

  private boolean isValidAnnualDays(boolean prevValid, Calendar cal)
  {
    boolean valid = prevValid;
    for (Iterator iter=annualDays.entrySet().iterator();iter.hasNext();)
    {
      AnnualDay ad = (AnnualDay)((Map.Entry)iter.next()).getValue();
      if (ad.day == cal.get(Calendar.DAY_OF_MONTH) && ad.month == cal.get(Calendar.MONTH))
      {
        valid = !exclude;
        break;
      }
    }
    return valid;
  }

  private boolean isValidDateRanges(boolean prevValid, Calendar cal)
  {
    String s = fmt.format(cal.getTime());
    boolean valid = prevValid;
    for (Iterator iter=dateRanges.entrySet().iterator();iter.hasNext();)
    {
      DateRange dr = (DateRange)((Map.Entry)iter.next()).getValue();
      if (dr.from.compareTo(s) <=0 && dr.to.compareTo(s) >=0)
      {
        valid = !exclude;
        break;
      }
    }
    return valid;
  }

  public boolean isValid(Date time, Map calendars)
  {
    boolean valid = !exclude;
    if (baseCalendarName != null)
    {
      SchedulerCalendar baseCalendar = (SchedulerCalendar)calendars.get(baseCalendarName);
      if (baseCalendar != null)
        valid = baseCalendar.isValid(time, calendars);
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(time);
    if (enableWeekDays)
    {
      valid = weekDays[cal.get(Calendar.DAY_OF_WEEK)-1];
      if (exclude)
        valid = !valid;
    }
    if (enableMonthDays)
    {
      if (enableMonthDayLast && cal.getLeastMaximum(Calendar.DAY_OF_MONTH) == cal.get(Calendar.DAY_OF_MONTH))
        valid = monthDays[LAST_DAY_OF_MONTH-1];
      else
        valid = monthDays[cal.get(Calendar.DAY_OF_MONTH)-1];
      if (exclude)
        valid = !valid;
    }
    if (enableAnnualDays)
      valid = isValidAnnualDays(valid,cal);
    if (enableDateRanges)
      valid = isValidDateRanges(valid,cal);
    return valid;
  }

  public boolean hasCalendarRef(String calendarName, Map calendars)
  {
    boolean ret = false;
    if (baseCalendarName != null)
    {
      ret = baseCalendarName.equals(calendarName);
      if (!ret)
      {
        SchedulerCalendar cal = (SchedulerCalendar)calendars.get(baseCalendarName);
        if (cal != null)
          ret = cal.hasCalendarRef(calendarName,calendars);
      }
    }
    return ret;
  }

  public SchedulerCalendar createCopy() throws Exception
  {
    return (SchedulerCalendar)Util.deepCopy(this);
  }

  private String arrayToString(boolean[] b)
  {
    StringBuffer buffer = new StringBuffer("[");
    for (int i=0;i<b.length;i++)
    {
      if (i>0)
        buffer.append(",");
      buffer.append(i);
      buffer.append("=");
      buffer.append(b[i]);
    }
    buffer.append("]");
    return buffer.toString();
  }
  public String toString()
  {
    return "[SchedulerCalendar, name="+name+", exclude="+exclude+
      ", enableWeekDays="+enableWeekDays+
      ", enableMonthDays="+enableMonthDays+
      ", enableMonthDayLast="+enableMonthDayLast+
      ", enableAnnualDays="+enableAnnualDays+
      ", enableDateRanges="+enableDateRanges+
      ", weekDays="+arrayToString(weekDays)+
      ", monthDays="+arrayToString(monthDays)+
      ", annualDays="+annualDays+
      ", dateRanges="+dateRanges+
      "]";
  }

  private class AnnualDay implements Serializable
  {
    int day = 0;
    int month = 0;

    public AnnualDay(int day, int month)
    {
      this.day = day;
      this.month = month;
    }

    public String toString()
    {
      return "[AnnualDay, day="+day+", month="+month+"]";
    }
  }

  private class DateRange implements Serializable
  {
    String from = null;
    String to = null;

    public DateRange(String from, String to)
    {
      this.from = from;
      this.to = to;
    }

    public String toString()
    {
      return "[DateRange, from="+from+", to="+to+"]";
    }
  }
}
