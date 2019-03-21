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

package com.swiftmq.impl.streams.comp.timer;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.processor.po.POTimer;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * At Timer implementation. Executes the onTimer callback at specific times (recurring).
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class AtTimer implements Timer {
    static final SimpleDateFormat timeFmt1 = new SimpleDateFormat("HH:mm:ss");
    static final SimpleDateFormat timeFmt2 = new SimpleDateFormat("HH:mm");

    StreamContext ctx;
    String name;
    TimerCallback callback;
    TimerListener timerListener;
    Entity usage = null;
    int callTime;
    List<Integer> times = new ArrayList<Integer>();
    int lastStart = 0;
    long nextStart = 0;

    AtTimer(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        try {
            EntityList timerList = (EntityList) ctx.usage.getEntity("timers");
            usage = timerList.createEntity();
            usage.setName(name);
            usage.createCommands();
            timerList.addEntity(usage);
            usage.getProperty("timer-type").setValue("At");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int timeToSeconds(String expr) throws Exception {
        // HH:mm[:ss]
        Date date = null;
        if (expr.length() == 5)
            date = timeFmt2.parse(expr);
        else if (expr.length() == 8)
            date = timeFmt1.parse(expr);
        if (date == null)
            throw new Exception("Invalid time format: " + expr + ", HH:mm:ss or HH:mm expected!");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
    }

    private int getTimeOfTheDay() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
    }

    private long calcNextStart() throws Exception {
        int timeOfDay = getTimeOfTheDay();
        boolean found = false;
        for (int i = 0; i < times.size(); i++) {
            int time = times.get(i);
            if (time >= timeOfDay && lastStart != time) {
                nextStart = (time - timeOfDay) * 1000;
                lastStart = time;
                found = true;
                break;
            }
        }
        if (!found) {
            if (times.size() > 0) {
                int time = times.get(0) + 86400;
                nextStart = (time - timeOfDay) * 1000;
            } else
                throw new Exception("No time has been set! Please use at(time)!");
        }
        return nextStart;
    }

    @Override
    public void executeCallback() throws Exception {
        if (callback != null) {
            long start = System.nanoTime();
            callback.execute(this);
            callTime = (int) (System.nanoTime() - start) / 1000;
            reconfigure();
        }
    }

    /**
     * Executes the Timer at a specific time. Format is hh:MM:ss or hh:MM.
     * This method can be called multiple times. The timer executes them
     * in order and daily recurring.
     *
     * @param expr time expression. Format is HH:mm:ss or HH:mm
     * @return this
     * @throws Exception on exception
     */
    public AtTimer time(String expr) throws Exception {
        times.add(timeToSeconds(expr));
        Collections.sort(times);
        return this;
    }

    @Override
    public void setTimerListener(TimerListener listener) {
        this.timerListener = listener;
    }

    @Override
    public TimerListener getTimerListener() {
        return timerListener;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Timer onTimer(TimerCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    public void collect(long interval) {
        try {
            usage.getProperty("timer-last-ontimer-time").setValue(new Integer(callTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() throws Exception {
        timerListener = new TimerListener() {
            @Override
            public void performTimeAction() {
                ctx.streamProcessor.dispatch(new POTimer(null, AtTimer.this));
            }
        };
        ctx.ctx.timerSwiftlet.addInstantTimerListener(calcNextStart(), timerListener);
        try {
            usage.getProperty("started").setValue(new Boolean(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public Timer reset() {
        nextStart = 0;
        return this;
    }

    @Override
    public void reconfigure() throws Exception {
        ctx.ctx.timerSwiftlet.removeTimerListener(timerListener);
        ctx.ctx.timerSwiftlet.addInstantTimerListener(calcNextStart(), timerListener);
    }

    @Override
    public void close() {
        try {
            if (usage != null)
                ctx.usage.getEntity("timers").removeEntity(usage);
            ctx.ctx.timerSwiftlet.removeTimerListener(timerListener);
            timerListener = null;
            callback = null;
            ctx.stream.removeTimer(this);
        } catch (Exception e) {
        }
    }

    @Override
    public String toString() {
        return "AtTimer{" +
                "name='" + name + '\'' +
                ", nextStart=" + nextStart +
                '}';
    }
}
