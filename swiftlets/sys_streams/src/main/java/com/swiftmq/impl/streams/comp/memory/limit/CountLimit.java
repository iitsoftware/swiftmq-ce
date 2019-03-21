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
import com.swiftmq.impl.streams.comp.memory.HeapMemory;
import com.swiftmq.impl.streams.comp.memory.Memory;
import com.swiftmq.impl.streams.comp.memory.RetirementCallback;

/**
 * A CountLimit keeps a Memory at a specific number of Messages and removes
 * older Messages if necessary.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class CountLimit implements Limit {
    StreamContext ctx;
    Memory memory;
    int n;
    boolean sliding = true;

    CountLimit(StreamContext ctx, Memory memory, int n) {
        this.ctx = ctx;
        this.memory = memory;
        this.n = n;
    }

    /**
     * Marks it as sliding count limit (default). If a new Message is being added
     * and the limit is reached, the oldest Message will retire.
     *
     * @return this
     */
    public CountLimit sliding() {
        sliding = true;
        return this;
    }

    /**
     * Marks it as tumbling count limit. If a new Message is being added and the limit
     * is reached, all Messages will retire and the Memory is empty.
     *
     * @return this
     */
    public CountLimit tumbling() {
        sliding = false;
        return this;
    }

    @Override
    public void checkLimit() {
        if (sliding)
            checkSlidingLimit();
        else
            checkTumbling();

    }

    private void checkTumbling() {
        try {
            if (memory.size() >= n) {
                Memory retired = null;
                RetirementCallback callback = memory.retirementCallback();
                if (callback != null) {
                    retired = new HeapMemory(ctx);
                    retired.orderBy(memory.orderBy());
                    for (int i = 0; i < n; i++) {
                        retired.add(memory.at(i));
                    }
                }
                for (int i = 0; i < n; i++) {
                    memory.remove(0);
                }
                if (callback != null)
                    callback.execute(retired);
            }
        } catch (Exception e) {
            ctx.logStackTrace(e);
        }
    }

    private void checkSlidingLimit() {
        try {
            if (memory.size() >= n) {
                int toRemove = memory.size() + 1 - n;
                Memory retired = null;
                RetirementCallback callback = memory.retirementCallback();
                if (callback != null) {
                    retired = new HeapMemory(ctx);
                    retired.orderBy(memory.orderBy());
                    for (int i = 0; i < toRemove; i++) {
                        retired.add(memory.at(i));
                    }
                }
                for (int i = 0; i < toRemove; i++) {
                    memory.remove(0);
                }
                if (callback != null)
                    callback.execute(retired);
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
        return "CountLimit{" +
                "n=" + n +
                '}';
    }
}
