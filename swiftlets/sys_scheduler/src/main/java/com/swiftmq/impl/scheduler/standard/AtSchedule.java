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

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class AtSchedule extends Schedule {
    int startTimes[] = null;
    boolean mayExpireWhileRouterDown = true;
    boolean firstScheduleAfterRouterStart = false;

    public AtSchedule(String name, boolean enabled, boolean loggingEnabled, String jobGroup, String jobName,
                      String calendar, String dateFrom, String dateTo, long maxRuntime, String timeExpression, int[] startTimes) {
        super(name, enabled, loggingEnabled, jobGroup, jobName, calendar, dateFrom, dateTo, maxRuntime, timeExpression);
        setStartTimes(startTimes);
    }

    protected void setMayExpireWhileRouterDown(boolean mayExpireWhileRouterDown) {
        this.mayExpireWhileRouterDown = mayExpireWhileRouterDown;
    }

    protected void setFirstScheduleAfterRouterStart(boolean firstScheduleAfterRouterStart) {
        this.firstScheduleAfterRouterStart = firstScheduleAfterRouterStart;
    }

    protected void setStartTimes(int[] startTimes) {
        this.startTimes = startTimes;
    }

    protected boolean isApplySystemTimeChange() {
        return false;
    }

    protected int getNextStart(int lastStartTime, Calendar cal) {
        int dayTime = getTimeOfTheDay(cal);
        for (int i = 0; i < startTimes.length; i++) {
            if (startTimes[i] >= dayTime && lastStartTime != startTimes[i]) {
                firstScheduleAfterRouterStart = false;
                return startTimes[i];
            }
        }
        int time = -1;
        if (!mayExpireWhileRouterDown) {
            if (firstScheduleAfterRouterStart)
                time = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND) + 1;
        }
        firstScheduleAfterRouterStart = false;
        return time;
    }

    private String formatStartTimes() {
        SimpleDateFormat fmt = new SimpleDateFormat("HH:mm:ss");
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < startTimes.length; i++) {
            if (i > 0)
                b.append(",");
            Calendar cal = Calendar.getInstance();
            cal.clear();
            cal.add(Calendar.SECOND, startTimes[i]);
            b.append(fmt.format(cal.getTime()));
        }
        return b.toString();
    }

    public String toString() {
        return "[AtSchedule, startTimes=" + formatStartTimes() + " " + super.toString() + "]";
    }
}
