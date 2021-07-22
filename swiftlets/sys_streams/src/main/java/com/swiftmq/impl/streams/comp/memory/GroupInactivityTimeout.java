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

/**
 * A GroupInactivityTimeout can be
 * attached to a MemoryGroup and specifies a time of inactivity (no adds to attached Memory)
 * after which all Messages in that Memory will retire and the Memory will be closed and
 * removed from the MemoryGroup. Inactivity is checked during MemoryGroup.checkLimit()
 * and thus needs to be regularly called from a Timer.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class GroupInactivityTimeout {
    StreamContext ctx;
    MemoryGroup memoryGroup;
    long millis;

    GroupInactivityTimeout(StreamContext ctx, MemoryGroup memoryGroup) {
        this.ctx = ctx;
        this.memoryGroup = memoryGroup;
    }

    /**
     * Adds n days to the timeout.
     *
     * @param n days
     * @return this
     */
    public GroupInactivityTimeout days(int n) {
        millis += 24 * 60 * 60 * (long) n * 1000;
        return this;
    }

    /**
     * Adds n hours to the timeout.
     *
     * @param n hours
     * @return this
     */
    public GroupInactivityTimeout hours(int n) {
        millis += 60 * 60 * (long) n * 1000;
        return this;
    }

    /**
     * Adds n minutes to the timeout.
     *
     * @param n minutes
     * @return this
     */
    public GroupInactivityTimeout minutes(int n) {
        millis += 60 * (long) n * 1000;
        return this;
    }

    /**
     * Adds n seconds to the timeout.
     *
     * @param n seconds
     * @return this
     */
    public GroupInactivityTimeout seconds(int n) {
        millis += (long) n * 1000;
        return this;
    }

    /**
     * Adds n milliseconds to the timeout.
     *
     * @param n milliseconds
     * @return this
     */
    public GroupInactivityTimeout milliseconds(long n) {
        millis += n;
        return this;
    }

    /**
     * Registers a MemoryCreateCallback at the MemoryGroup
     *
     * @param callback MemoryCreateCallback
     * @return MemoryGroup
     */
    public MemoryGroup onCreate(MemoryCreateCallback callback) {
        return memoryGroup.onCreate(callback);
    }

    /**
     * Registers a MemoryRemoveCallback at the MemoryGroup
     *
     * @param callback MemoryRemoveCallback
     * @return MemoryGroup
     */
    public MemoryGroup onRemove(MemoryRemoveCallback callback) {
        return memoryGroup.onRemove(callback);
    }

    long getMillis() {
        return millis;
    }

    @Override
    public String toString() {
        return "GroupInactivityTimeout {" +
                "millis=" + millis +
                '}';
    }
}
