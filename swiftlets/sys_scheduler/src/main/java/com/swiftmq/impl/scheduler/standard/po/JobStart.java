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

package com.swiftmq.impl.scheduler.standard.po;

import com.swiftmq.impl.scheduler.standard.Scheduler;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class JobStart extends POObject implements TimerListener {
    String name = null;
    long startTime = 0;
    int startTimeSec = 0;
    long maxRuntime = 0;
    Scheduler scheduler = null;

    public JobStart(String name, long startTime, int startTimeSec, long maxRuntime) {
        super(null, null);
        this.name = name;
        this.startTime = startTime;
        this.startTimeSec = startTimeSec;
        this.maxRuntime = maxRuntime;
    }

    public String getName() {
        return name;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getStartTimeSec() {
        return startTimeSec;
    }

    public long getMaxRuntime() {
        return maxRuntime;
    }

    public void accept(POVisitor poVisitor) {
        ((EventVisitor) poVisitor).visit(this);
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void performTimeAction() {
        scheduler.enqueue(this);
    }

    public String toString() {
        return "[JobStart, name=" + name + "]";
    }
}
