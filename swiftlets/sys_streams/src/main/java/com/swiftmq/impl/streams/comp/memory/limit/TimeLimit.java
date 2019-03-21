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
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.memory.RetirementCallback;

/**
 * A TimeLimit keeps a Memory at a specific time window (e.g. last 30 secs) and removes
 * older Messages if necessary.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class TimeLimit implements Limit {
    StreamContext ctx;
    Memory memory;
    long millis;
    boolean sliding = true;

    TimeLimit(StreamContext ctx, Memory memory) {
        this.ctx = ctx;
        this.memory = memory;
    }

    /**
     * Marks it as sliding count limit (default). If a new Message is being added
     * and the limit is reached, the oldest Message will retire.
     *
     * @return this
     */
    public TimeLimit sliding() {
        sliding = true;
        return this;
    }

    /**
     * Marks it as tumbling count limit. If a new Message is being added and the limit
     * is reached, all Messages will retire and the Memory is empty.
     *
     * @return this
     */
    public TimeLimit tumbling() {
        sliding = false;
        return this;
    }

    /**
     * Adds n days to the limit.
     *
     * @param n days
     * @return TimeLimit
     */
    public TimeLimit days(int n) {
        millis += 24 * 60 * 60 * n * 1000;
        return this;
    }

    /**
     * Adds n hours to the limit.
     *
     * @param n hours
     * @return TimeLimit
     */
    public TimeLimit hours(int n) {
        millis += 60 * 60 * n * 1000;
        return this;
    }

    /**
     * Adds n minutes to the limit.
     *
     * @param n minutes
     * @return TimeLimit
     */
    public TimeLimit minutes(int n) {
        millis += 60 * n * 1000;
        return this;
    }

    /**
     * Adds n seconds to the limit.
     *
     * @param n seconds
     * @return TimeLimit
     */
    public TimeLimit seconds(int n) {
        millis += n * 1000;
        return this;
    }

    /**
     * Adds n milliseconds to the limit.
     *
     * @param n milliseconds
     * @return TimeLimit
     */
    public TimeLimit milliseconds(long n) {
        millis += n;
        return this;
    }

    @Override
    public void checkLimit() {
        try {
            if (memory.size() == 0)
                return;
            long time;
            if (memory.orderBy() != null)
                time = memory.getStoreTime(memory.size()-1) - millis;
            else
                time = System.currentTimeMillis() - millis;
            if (sliding) {
                memory.removeOlderThan(time, true);
            } else {
                if (memory.getStoreTime(0) <= time)
                    memory.removeOlderThan(memory.getStoreTime(0) + millis, true);
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
        return "TimeLimit{" +
                "millis=" + millis +
                '}';
    }
}
