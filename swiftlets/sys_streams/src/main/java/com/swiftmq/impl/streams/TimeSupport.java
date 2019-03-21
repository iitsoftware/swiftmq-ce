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

package com.swiftmq.impl.streams;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Convenience class that provides various time methods. It is accessible from
 * Streams scripts via variable "time".
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class TimeSupport {

    /**
     * Returns the current System time
     *
     * @return system time
     */
    public long currentTime() {
        return System.currentTimeMillis();
    }

    /**
     * Formats the time according to the pattern.
     *
     * @param time    time
     * @param pattern pattern (see SimpleDateFormat)
     * @return formatted time
     */
    public String format(long time, String pattern) {
        SimpleDateFormat fmt = new SimpleDateFormat(pattern);
        return fmt.format(new Date(time));
    }

    /**
     * Parses a formatted date/time string and returns the time in milliseconds.
     *
     * @param datetime formatted date/time string
     * @param pattern  pattern (see SimpleDateFormat)
     * @return time in millis
     * @throws Exception
     */
    public long parse(String datetime, String pattern) throws Exception {
        SimpleDateFormat fmt = new SimpleDateFormat(pattern);
        return fmt.parse(datetime).getTime();
    }

    /**
     * Returns the seconds from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return seconds
     */
    public int second(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.SECOND, offset);
        return calendar.get(Calendar.SECOND);
    }

    /**
     * Returns the seconds of the current time.
     *
     * @return seconds
     */
    public int second() {
        return second(currentTime(), 0);
    }

    /**
     * Returns the start of the second from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of seconds in milliseconds
     */
    public long startOfSecond(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        if (offset != 0)
            calendar.add(Calendar.SECOND, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the minute from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return minute
     */
    public int minute(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.MINUTE, offset);
        return calendar.get(Calendar.MINUTE);
    }

    /**
     * Returns the minute of the current time.
     *
     * @return minute
     */
    public int minute() {
        return minute(currentTime(), 0);
    }

    /**
     * Returns the start of the minute from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of minute in milliseconds
     */
    public long startOfMinute(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        if (offset != 0)
            calendar.add(Calendar.MINUTE, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the hour from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return hour
     */
    public int hour(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.HOUR_OF_DAY, offset);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * Returns the hour of the current time.
     *
     * @return hour
     */
    public int hour() {
        return hour(currentTime(), 0);
    }

    /**
     * Returns the start of the hour from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of hour in milliseconds
     */
    public long startOfHour(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        if (offset != 0)
            calendar.add(Calendar.HOUR_OF_DAY, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the day from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return day
     */
    public int day(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * Returns the day of the current time.
     *
     * @return day
     */
    public int day() {
        return day(currentTime(), 0);
    }

    /**
     * Returns the start of the day from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of day in milliseconds
     */
    public long startOfDay(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        if (offset != 0)
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the display name of the day (e.g. "Tuesday") from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return day display name
     */
    public String dayDisplayName(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.DAY_OF_WEEK, offset);
        return calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.getDefault());
    }

    /**
     * Returns the display name of the day (e.g. "Tuesday") of the current time.
     *
     * @return day display name
     */
    public String dayDisplayName() {
        return dayDisplayName(currentTime(), 0);
    }

    /**
     * Returns the week from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return week
     */
    public int week(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.WEEK_OF_YEAR, offset);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * Returns the week of the current time.
     *
     * @return week
     */
    public int week() {
        return week(currentTime(), 0);
    }

    /**
     * Returns the start of the week from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of week in milliseconds
     */
    public long startOfWeek(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek());
        if (offset != 0)
            calendar.add(Calendar.WEEK_OF_YEAR, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the month from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return month
     */
    public int month(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.MONTH, offset);
        return calendar.get(Calendar.MONTH);
    }

    /**
     * Returns the month of the current time.
     *
     * @return month
     */
    public int month() {
        return month(currentTime(), 0);
    }

    /**
     * Returns the start of the month from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of month in milliseconds
     */
    public long startOfMonth(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        if (offset != 0)
            calendar.add(Calendar.MONTH, offset);
        return calendar.getTimeInMillis();
    }

    /**
     * Returns the display name of the month (e.g. "February") from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return month display name
     */
    public String monthDisplayName(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (offset != 0)
            calendar.add(Calendar.MONTH, offset);
        return calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.getDefault());
    }

    /**
     * Returns the display name of the month (e.g. "February") of the current time.
     *
     * @return month display name
     */
    public String monthDisplayName() {
        return monthDisplayName(currentTime(), 0);
    }

    /**
     * Returns the year from the time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return year
     */
    public int year(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return calendar.get(Calendar.YEAR);
    }

    /**
     * Returns the year of the current time.
     *
     * @return year
     */
    public int year() {
        return year(currentTime(), 0);
    }

    /**
     * Returns the start of the year from time+offset. Offset can be positive or negative.
     *
     * @param time   time
     * @param offset offset
     * @return start of year in milliseconds
     */
    public long startOfYear(long time, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_YEAR, 1);
        calendar.set(Calendar.MONTH, 0);
        if (offset != 0)
            calendar.add(Calendar.YEAR, offset);
        return calendar.getTimeInMillis();
    }
}
