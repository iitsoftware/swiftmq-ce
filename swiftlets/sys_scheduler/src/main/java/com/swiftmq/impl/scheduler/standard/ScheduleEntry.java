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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ScheduleEntry implements Serializable {
    static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
    static final String NOW = "now";
    static final String FOREVER = "forever";
    String name = null;
    String calendar = null;
    String dateFrom = null;
    String dateTo = null;

    public ScheduleEntry(String name, String calendar, String dateFrom, String dateTo) {
        this.name = name;
        this.calendar = calendar;
        this.dateFrom = dateFrom;
        this.dateTo = dateTo;
    }

    public String getName() {
        return name;
    }

    public String getCalendar() {
        return calendar;
    }

    public void setCalendar(String calendar) {
        this.calendar = calendar;
    }

    public String getDateFrom() {
        return dateFrom;
    }

    public void setDateFrom(String dateFrom) {
        this.dateFrom = dateFrom;
    }

    public String getDateTo() {
        return dateTo;
    }

    public void setDateTo(String dateTo) {
        this.dateTo = dateTo;
    }

    public boolean isInDateRange(long time) {
        boolean from = false;
        boolean to = false;
        if (dateFrom.equals(NOW))
            from = true;
        else {
            try {
                from = fmt.parse(dateFrom).getTime() <= time;
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        if (dateTo.equals(FOREVER))
            to = true;
        else {
            try {
                to = fmt.parse(dateTo).getTime() >= time;
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return from && to;
    }
}
