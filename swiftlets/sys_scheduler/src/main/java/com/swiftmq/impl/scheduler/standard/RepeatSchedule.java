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

package com.swiftmq.impl.scheduler.standard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RepeatSchedule extends AtSchedule {
    int startTime = 0;
    int endTime = 0;
    int delay = 0;
    int repeats = 0;

    public RepeatSchedule(String name, boolean enabled, boolean loggingEnabled, String jobGroup, String jobName, String calendar,
                          String dateFrom, String dateTo, long maxRuntime, String timeExpression, int startTime, int endTime, int delay, int repeats) {
        super(name, enabled, loggingEnabled, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpression, null);
        this.startTime = startTime;
        this.endTime = endTime;
        this.delay = delay;
        this.repeats = repeats;
        computeStartTimes();
    }

    private void computeStartTimes() {
        List<Integer> list = new ArrayList<>();
        int fullDay = 24 * 3600;
        int actTime = startTime;
        int actEnd = endTime < actTime ? endTime + fullDay : endTime;
        int repeatCount = 0;
        do {
            list.add(actTime >= fullDay ? actTime - fullDay : actTime);
            actTime += delay;
            repeatCount++;
        } while ((actTime <= actEnd) && (repeats == -1 || repeatCount < repeats));
        Collections.sort(list);
        int[] st = new int[list.size()];
        Arrays.setAll(st, list::get);
        setStartTimes(st);
    }

    protected boolean isApplySystemTimeChange() {
        return true;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
        computeStartTimes();
    }

    public void setEndTime(int endTime) {
        this.endTime = endTime;
    }

    public void setDelay(int delay) {
        this.delay = delay;
        computeStartTimes();
    }

    public void setRepeats(int repeats) {
        this.repeats = repeats;
        computeStartTimes();
    }

    public String toString() {
        return "[RepeatSchedule startTime=" + startTime + ", endTime=" + endTime + ", delay=" + delay + ", repeats=" + repeats + " " + super.toString() + "]";
    }
}
