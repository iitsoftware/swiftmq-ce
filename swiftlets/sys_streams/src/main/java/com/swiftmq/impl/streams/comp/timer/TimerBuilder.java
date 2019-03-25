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

/**
 * Factory to create Timers.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class TimerBuilder {
    StreamContext ctx;
    String name;

    /**
     * Internal use.
     */
    public TimerBuilder(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    /**
     * Creates an IntervalTimer.
     *
     * @return IntervalTimer
     */
    public IntervalTimer interval() {
        return (IntervalTimer) ctx.stream.addTimer(name, new IntervalTimer(ctx, name));
    }

    /**
     * Creates an AtTimer.
     *
     * @return AtTimer
     */
    public AtTimer at() {
        return (AtTimer) ctx.stream.addTimer(name, new AtTimer(ctx, name));
    }

    /**
     * Creates a NextTimer.
     *
     * @return NextTimer
     */
    public NextTimer next() {
        return (NextTimer) ctx.stream.addTimer(name, new NextTimer(ctx, name));
    }
}
