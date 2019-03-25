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

/**
 * A JobTerminationListener is passed to the start method of a Job and has to be
 * called when the Job has finished his work. It must not called if the stop method
 * of the Job has been called in the meantime.
 * <br><br>
 * After calling one of the 'jobTerminated' methods, the Job is marked as finished.
 * Whether the Job is re-scheduled or not depedents whether a JobException has been
 * passed and whether the JobException's flag 'furtherSchedulesAllowed' is true.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public interface JobTerminationListener {
    /**
     * Marks the Job as finished and re-schedules it.
     */
    public void jobTerminated();

    /**
     * Marks the Job as finished and re-schedules it. The passed message is informational
     * and will be stored in the Job history / log file. It should contain some infos
     * about the result, e.g. '200 Messages moved'.
     */
    public void jobTerminated(String message);

    /**
     * Marks the Job as finished. Whether the Job is re-scheduled or not depedents whether the JobException's
     * flag 'furtherSchedulesAllowed' is true.
     *
     * @param exception
     */
    public void jobTerminated(JobException exception);

}
