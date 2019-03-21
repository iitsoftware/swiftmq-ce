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

/**
 * Interval Timer implementation. Executes the onTimer callback in an interval.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class IntervalTimer implements Timer {
    StreamContext ctx;
    String name;
    TimerCallback callback;
    long millis;
    TimerListener timerListener;
    int callTime = 0;
    Entity usage = null;

    IntervalTimer(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        try {
            EntityList timerList = (EntityList) ctx.usage.getEntity("timers");
            usage = timerList.createEntity();
            usage.setName(name);
            usage.createCommands();
            timerList.addEntity(usage);
            usage.getProperty("timer-type").setValue("Interval");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Adds n days to the interval.
     *
     * @param n days
     * @return IntervalTimer
     */
    public IntervalTimer days(int n) {
        millis += 24L * 60L * 60L * (long) n * 1000L;
        return this;
    }

    /**
     * Adds n hours to the interval.
     *
     * @param n hours
     * @return IntervalTimer
     */
    public IntervalTimer hours(int n) {
        millis += 60L * 60L * (long) n * 1000L;
        return this;
    }

    /**
     * Adds n minutes to the interval.
     *
     * @param n minutes
     * @return IntervalTimer
     */
    public IntervalTimer minutes(int n) {
        millis += 60L * (long) n * 1000L;
        return this;
    }

    /**
     * Adds n seconds to the interval.
     *
     * @param n seconds
     * @return IntervalTimer
     */
    public IntervalTimer seconds(int n) {
        millis += (long) n * 1000L;
        return this;
    }

    /**
     * Adds n days to the milliseconds.
     *
     * @param n milliseconds
     * @return IntervalTimer
     */
    public IntervalTimer milliseconds(long n) {
        millis += n;
        return this;
    }

    @Override
    public void executeCallback() throws Exception {
        if (callback != null) {
            long start = System.nanoTime();
            callback.execute(this);
            callTime = (int) (System.nanoTime() - start) / 1000;
        }
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
                ctx.streamProcessor.dispatch(new POTimer(null, IntervalTimer.this));
            }
        };
        ctx.ctx.timerSwiftlet.addTimerListener(millis, timerListener);
        try {
            usage.getProperty("started").setValue(new Boolean(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public Timer reset() {
        millis = 0;
        return this;
    }

    @Override
    public void reconfigure() throws Exception {
        ctx.ctx.timerSwiftlet.removeTimerListener(timerListener);
        ctx.ctx.timerSwiftlet.addTimerListener(millis, timerListener);
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
        return "IntervalTimer{" +
                "name='" + name + '\'' +
                ", millis=" + millis +
                '}';
    }
}
