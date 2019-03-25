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

package com.swiftmq.tools.timer;

import java.util.EventObject;

/**
 * An event fired from Timers
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class TimerEvent extends EventObject {
    byte timePoint = 0;
    long delay = 0;

    public TimerEvent(Object source, long delay) {
        super(source);
        this.delay = delay;
    }

    public TimerEvent(Object source, byte timePoint) {
        super(source);
        this.timePoint = timePoint;
    }

    public long getDelay() {
        return delay;
    }

    public byte getTimePoint() {
        return timePoint;
    }

    public String toString() {
        return "[TimerEvent, source=" + source + ", delay=" + delay + ", timePoint=" + timePoint + "]";
    }
}
