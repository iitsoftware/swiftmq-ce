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

import java.util.Calendar;

/**
 * Next Timer implementation. Executes the onTimer callback once at the begin of a specific time.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class NextTimer implements Timer {
    final static int DAY = 0;
    final static int HOUR = 1;
    final static int MINUTE = 2;
    final static int SECOND = 3;
    StreamContext ctx;
    String name;
    TimerCallback callback;
    long millis;
    TimerListener timerListener;
    Entity usage = null;
    int type = MINUTE;

    NextTimer(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        try {
            EntityList timerList = (EntityList) ctx.usage.getEntity("timers");
            usage = timerList.createEntity();
            usage.setName(name);
            usage.createCommands();
            timerList.addEntity(usage);
            usage.getProperty("timer-type").setValue("Next");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeCallback() throws Exception {
        if (callback != null) {
            callback.execute(this);
            close();
        }
    }

    /**
     * Starts the timer at the begin of the next day
     *
     * @return this
     */
    public NextTimer beginOfDay() {
        type = DAY;
        return this;
    }

    /**
     * Starts the timer at the begin of the next hour
     *
     * @return this
     */
    public NextTimer beginOfHour() {
        type = HOUR;
        return this;
    }

    /**
     * Starts the timer at the begin of the next minute
     *
     * @return this
     */
    public NextTimer beginOfMinute() {
        type = MINUTE;
        return this;
    }

    /**
     * Starts the timer at the begin of the next second
     *
     * @return this
     */
    public NextTimer beginOfSecond() {
        type = SECOND;
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
    }

    @Override
    public void start() throws Exception {
        timerListener = new TimerListener() {
            @Override
            public void performTimeAction() {
                ctx.streamProcessor.dispatch(new POTimer(null, NextTimer.this));
            }
        };
        computeTime();
        ctx.ctx.timerSwiftlet.addInstantTimerListener(millis, timerListener);
        try {
            usage.getProperty("started").setValue(new Boolean(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void computeTime() {
        Calendar cal = Calendar.getInstance();
        switch (type) {
            case DAY:
                cal.add(Calendar.DATE, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case HOUR:
                cal.add(Calendar.HOUR_OF_DAY, 1);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case MINUTE:
                cal.add(Calendar.MINUTE, 1);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                break;
            case SECOND:
                cal.add(Calendar.SECOND, 1);
                cal.set(Calendar.MILLISECOND, 0);
                break;
        }
        millis = cal.getTimeInMillis() - System.currentTimeMillis();
    }

    @Override
    public Timer reset() {
        millis = 0;
        return this;
    }

    @Override
    public void reconfigure() throws Exception {
        ctx.ctx.timerSwiftlet.removeTimerListener(timerListener);
        computeTime();
        ctx.ctx.timerSwiftlet.addInstantTimerListener(millis, timerListener);
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
        return "NextTimer{" +
                "name='" + name + '\'' +
                ", millis=" + millis +
                '}';
    }
}
