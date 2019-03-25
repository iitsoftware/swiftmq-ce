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

package com.swiftmq.impl.streams.comp.memory;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.comp.memory.limit.LimitBuilder;

/**
 * InactivityTimeout can be attached to a Memory and specifies a time of inactivity (no adds to Memory)
 * after which all Messages in that Memory will retire. Inactivity is checked during Memory.checkLimit()
 * and thus needs to be regularly called from a Timer.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class InactivityTimeout {
    StreamContext ctx;
    Memory memory;
    long millis;

    InactivityTimeout(StreamContext ctx, Memory memory) {
        this.ctx = ctx;
        this.memory = memory;
    }

    /**
     * Adds n days to the timeout.
     *
     * @param n days
     * @return this
     */
    public InactivityTimeout days(int n) {
        millis += 24 * 60 * 60 * n * 1000;
        return this;
    }

    /**
     * Adds n hours to the timeout.
     *
     * @param n hours
     * @return this
     */
    public InactivityTimeout hours(int n) {
        millis += 60 * 60 * n * 1000;
        return this;
    }

    /**
     * Adds n minutes to the timeout.
     *
     * @param n minutes
     * @return this
     */
    public InactivityTimeout minutes(int n) {
        millis += 60 * n * 1000;
        return this;
    }

    /**
     * Adds n seconds to the timeout.
     *
     * @param n seconds
     * @return this
     */
    public InactivityTimeout seconds(int n) {
        millis += n * 1000;
        return this;
    }

    /**
     * Adds n milliseconds to the timeout.
     *
     * @param n milliseconds
     * @return this
     */
    public InactivityTimeout milliseconds(long n) {
        millis += n;
        return this;
    }

    /**
     * Sets the RetirementCallback at the Memory
     *
     * @param callback RetirementCallback
     * @return this
     */
    public InactivityTimeout onRetire(RetirementCallback callback) {
        memory.onRetire(callback);
        return this;
    }

    /**
     * Returns a LimitBuilder to add Limits to the Memory
     *
     * @return LimitBuilder
     */
    public LimitBuilder limit() {
        return memory.limit();
    }

    long getMillis() {
        return millis;
    }

    @Override
    public String toString() {
        return "InactivityTimeout {" +
                "millis=" + millis +
                '}';
    }
}
