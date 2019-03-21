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

import com.swiftmq.swiftlet.timer.event.TimerListener;

/**
 * Base interface for Timers.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public interface Timer {

    /**
     * Internal use.
     */
    public void setTimerListener(TimerListener listener);

    /**
     * Internal use.
     */
    public TimerListener getTimerListener();

    /**
     * Returns the name of the Timer.
     *
     * @return Name
     */
    public String name();

    /**
     * Sets the onTimer callback.
     *
     * @param callback callback
     * @return Timer
     */
    public Timer onTimer(TimerCallback callback);

    /**
     * Internal use.
     */
    public void executeCallback() throws Exception;

    /**
     * Internal use
     */
    public void collect(long interval);

    /**
     * Starts this Timer. This method is called automatically if a Timer is created
     * outside a callback. If it is created inside, it must be called explicitly.
     */
    public void start() throws Exception;

    /**
     * Resets the value of a Timer
     */
    public Timer reset();

    /**
     * Reconfigures this Timer and applies new settings
     */
    public void reconfigure() throws Exception;

    /**
     * Closes this Timer.
     */
    public void close();

}
