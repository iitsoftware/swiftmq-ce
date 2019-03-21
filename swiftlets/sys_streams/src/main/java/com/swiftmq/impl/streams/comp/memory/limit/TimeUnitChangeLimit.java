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

package com.swiftmq.impl.streams.comp.memory.limit;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.TimeSupport;
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.memory.RetirementCallback;

import java.util.Calendar;

/**
 * A TimeUnitChangeLimit keeps a Memory at a specific time unit window (e.g. 1 hour based on midnight) and removes
 * older Messages if necessary.
 * <p>
 * The time unit window's time is based as follows (MM/DD/YYYY):
 * <ul>
 * <li>seconds: based on minute, e.g. 10 secs at time 18:12:17 starts at 18:12:10</li>
 * <li>minutes: based on hour, e.g. 5 mins at time 18:12:17 starts at 18:10:00</li>
 * <li>days: based on month, e.g. 2 days at 04/17/2017 starts at 04/16/2017</li>
 * <li>weeks: based on year, e.g. 2 weeks at 01/15/2017 starts at 01/14/2017</li>
 * <li>months: based on year, e.g. 3 month at 05/10/2017 starts at 04/01/2017</li>
 * <li>years: based on year, e.g. 2 years at 05/10/2017 starts at 01/01/2017</li>
 * </ul>
 * </p>
 * <p>
 * The difference between this time unit limit and a time limit is that the time limit has the window base at
 * the time when it is started, e.g. started at 18:12:17 with a 10 secs window has a window from 18:12:17 to 18:12:27
 * exclusive.
 * </p>
 * <p>
 * A time unit change limit, however, has a fixed base and synchronizes the first window to it. For example,
 * a 10 secs window started at 18:12:17 has a window start at 18:12:10 and ends therefore at 18:12:20 (exclusive).
 * So this first window is only 13 secs long. The following windows will have full 10 secs. This is useful
 * to synchronize events on fixed base times (full seconds/minutes/hours etc).
 * </p>
 * <p>
 * A time unit limt can only have one time unit, e.g. it is not possible to chain units like seconds(10).minutes(5).
 * </p>
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class TimeUnitChangeLimit implements Limit {
    StreamContext ctx;
    Memory memory;
    TimeSupport timeSupport = new TimeSupport();
    int units;
    int field;
    long windowEnd = -1;
    long lastUnit = -1;

    /**
     * Internal use only.
     */
    public TimeUnitChangeLimit(StreamContext ctx, Memory memory) {
        this.ctx = ctx;
        this.memory = memory;
    }

    /**
     * Sets a seconds time unit limit
     *
     * @param n number of seconds
     * @return this
     */
    public TimeUnitChangeLimit seconds(int n) {
        this.units = n;
        field = Calendar.SECOND;
        return this;
    }

    /**
     * Sets a minutes time unit limit
     *
     * @param n number of minutes
     * @return this
     */
    public TimeUnitChangeLimit minutes(int n) {
        this.units = n;
        field = Calendar.MINUTE;
        return this;
    }

    /**
     * Sets a hours time unit limit
     *
     * @param n number of hours
     * @return this
     */
    public TimeUnitChangeLimit hours(int n) {
        this.units = n;
        field = Calendar.HOUR_OF_DAY;
        return this;
    }

    /**
     * Sets a days time unit limit
     *
     * @param n number of days
     * @return this
     */
    public TimeUnitChangeLimit days(int n) {
        this.units = n;
        field = Calendar.DAY_OF_MONTH;
        return this;
    }

    /**
     * Sets a weeks time unit limit
     *
     * @param n number of weeks
     * @return this
     */
    public TimeUnitChangeLimit weeks(int n) {
        this.units = n;
        field = Calendar.WEEK_OF_YEAR;
        return this;
    }

    /**
     * Sets a months time unit limit
     *
     * @param n number of months
     * @return this
     */
    public TimeUnitChangeLimit months(int n) {
        this.units = n;
        field = Calendar.MONTH;
        return this;
    }

    /**
     * Sets a years time unit limit
     *
     * @param n number of years
     * @return this
     */
    public TimeUnitChangeLimit years(int n) {
        this.units = n;
        field = Calendar.YEAR;
        return this;
    }

    private long calcTime(long time, int units) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        switch (field) {
            case Calendar.SECOND:
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case Calendar.MINUTE:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                break;
            case Calendar.HOUR_OF_DAY:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MINUTE, 0);
                break;
            case Calendar.DAY_OF_MONTH:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                break;
            case Calendar.WEEK_OF_YEAR:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
                break;
            case Calendar.MONTH:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                break;
            case Calendar.YEAR:
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MONTH, 0);
                cal.set(Calendar.DAY_OF_YEAR, 1);
                break;
        }
        if (field != Calendar.YEAR)
            cal.set(field, (cal.get(field) / units) * units);
        cal.add(field, units);
        return cal.getTimeInMillis();
    }

    private long currentUnit(long time) {
        return calcTime(time, units);
    }

    private void calcWindow(long time) {
        if (windowEnd == -1) {
            windowEnd = calcTime(time, units);
            lastUnit = windowEnd;
        }

    }

    @Override
    public void checkLimit() {
        try {
            if (memory.size() == 0)
                return;
            calcWindow(memory.getStoreTime(0));
            long currentUnit = currentUnit(memory.orderBy() != null ? memory.getStoreTime(memory.size() - 1) : System.currentTimeMillis());
            if (currentUnit != lastUnit) {
                memory.removeOlderThan(windowEnd, true);
                windowEnd = -1;
            }
        } catch (Exception e) {
            ctx.logStackTrace(e);
        }
    }

    @Override
    public Limit onRetire(RetirementCallback callback) {
        memory.onRetire(callback);
        return this;
    }

    @Override
    public LimitBuilder limit() {
        return memory.limit();
    }

    @Override
    public String toString() {
        return "TimeUnitChangeLimit{" +
                "units=" + units +
                ", field=" + field +
                ", windowEnd=" + windowEnd +
                ", lastUnit=" + lastUnit +
                '}';
    }
}
