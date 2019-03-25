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

package com.swiftmq.swiftlet.timer;

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.timer.event.SystemTimeChangeListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;

/**
 * The TimerSwiftlet manages timers for a SwiftMQ router.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2009, All Rights Reserved
 */
public abstract class TimerSwiftlet extends Swiftlet {
    /**
     * Add a new instant TimerListener for a specific delay. An instant
     * TimerListener run exactly once so a removeTimerListener isn't necessary.
     *
     * @param delay    delay time in ms
     * @param listener timer listener
     */
    public abstract void addInstantTimerListener(long delay, TimerListener listener);

    /**
     * Add a new instant TimerListener for a specific delay. An instant
     * TimerListener run exactly once so a removeTimerListener isn't necessary.
     * The TimerListener will be dispatched into the thread pool supplied as a parameter.
     *
     * @param delay      delay time in ms
     * @param threadpool thread pool
     * @param listener   timer listener
     */
    public abstract void addInstantTimerListener(long delay, ThreadPool threadpool, TimerListener listener);

    /**
     * Add a new instant TimerListener for a specific delay. An instant
     * TimerListener run exactly once so a removeTimerListener isn't necessary.
     *
     * @param delay                       delay time in ms
     * @param listener                    timer listener
     * @param doNotApplySystemTimeChanges if true, do NOT apply system time changes
     */
    public abstract void addInstantTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges);

    /**
     * Add a new TimerListener for a specific delay
     *
     * @param delay    delay time in ms
     * @param listener timer listener
     */
    public abstract void addTimerListener(long delay, TimerListener listener);

    /**
     * Add a new TimerListener for a specific delay
     *
     * @param delay                       delay time in ms
     * @param listener                    timer listener
     * @param doNotApplySystemTimeChanges if true, do NOT apply system time changes
     */
    public abstract void addTimerListener(long delay, TimerListener listener, boolean doNotApplySystemTimeChanges);

    /**
     * Add a new TimerListener for a specific delay
     * The TimerListener will be dispatched into the thread pool supplied as a parameter.
     *
     * @param delay      delay time in ms
     * @param threadpool thread pool
     * @param listener   timer listener
     */
    public abstract void addTimerListener(long delay, ThreadPool threadpool, TimerListener listener);

    /**
     * Add a new TimerListener for a specific delay
     * The TimerListener will be dispatched into the thread pool supplied as a parameter.
     *
     * @param delay                       delay time in ms
     * @param threadpool                  thread pool
     * @param listener                    timer listener
     * @param doNotApplySystemTimeChanges if true, do NOT apply system time changes
     */
    public abstract void addTimerListener(long delay, ThreadPool threadpool, TimerListener listener, boolean doNotApplySystemTimeChanges);

    /**
     * Remove a registered TimerListener
     *
     * @param listener timer listener
     */
    public abstract void removeTimerListener(TimerListener listener);

    /**
     * Add a new SystemTimeChangeListener
     *
     * @param listener system time change listener
     */
    public abstract void addSystemTimeChangeListener(SystemTimeChangeListener listener);

    /**
     * Remove a registered SystemTimeChangeListener
     *
     * @param listener system time change listener
     */
    public abstract void removeSystemTimeChangeListener(SystemTimeChangeListener listener);
}

