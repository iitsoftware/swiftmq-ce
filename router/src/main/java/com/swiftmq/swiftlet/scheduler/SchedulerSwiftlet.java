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

package com.swiftmq.swiftlet.scheduler;

import com.swiftmq.swiftlet.Swiftlet;

/**
 * The Scheduler Swiftlet is responsible to schedule Jobs. To this, JobFactories are registered
 * via a JobGroup. The actual scheduling is usually done by the user via the Scheduler Swiftlet
 * configuration. During a schedule, a Job object is created out of the JobFactory. It is also
 * possible to add and remove temporary schedules programmatically. A temporary schedule is a
 * schedule which isn't saved to the routerconfig.xml.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public abstract class SchedulerSwiftlet extends Swiftlet {
    /**
     * Returns the JobGroup with the given name. If the JobGroup doesn't exists, it will
     * be created and stored. Subsequent calls will return the same JobGroup object.
     *
     * @param name Name of the JobGroup
     * @return JobGroup
     */
    public abstract JobGroup getJobGroup(String name);

    /**
     * Add a new temporary schedule. A temporary schedule is a schedule which isn't saved
     * to the routerconfig.xml. Instead it must be created and removed programmatically.
     *
     * @param name           The name of the schedule
     * @param jobGroup       Name of the job group
     * @param jobName        Name of the job
     * @param calendar       Name of the calendar to use (optional)
     * @param dateFrom       From date in format "yyyy-MM-dd" (optional)
     * @param dateTo         To date in format "yyyy-MM-dd" (optional)
     * @param timeExpression Time expression in format "('at' HH:mm[:ss][, HH:mm[:ss]...]) | ('start' HH:mm[:ss] 'stop' HH:mm[:ss] 'delay' n('s'|'m'|'h' ['repeat' n])"
     * @param maxRuntime     Max. runtime in format "n('s'|'m'|'h')"
     * @param loggingEnabled States whether the start/stop of jobs should be logged in SwiftMQ's log files
     * @throws InvalidScheduleException if the schedule is invalid
     */
    public abstract void addTemporarySchedule(String name, String jobGroup,
                                              String jobName, String calendar,
                                              String dateFrom, String dateTo,
                                              String timeExpression, String maxRuntime,
                                              boolean loggingEnabled) throws InvalidScheduleException;

    /**
     * Remove a temporary schedule.
     *
     * @param name The name of the schedule
     * @return true if the schedule was found and removed
     */
    public abstract boolean removeTemporarySchedule(String name);
}
