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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A JobException is thrown by a Job or passed from a Job to a JobTerminationListener.
 * It contains an optional nested exception and an attribute which decides whether the
 * Job can be rescheduled or is marked as errorneous.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public class JobException extends Exception {
    Exception nestedException = null;
    boolean furtherScheduleAllowed = true;

    /**
     * Creates a JobException with a message and a nested exception. Further schedules
     * are allowed.
     *
     * @param s               Message
     * @param nestedException nested exception
     */
    public JobException(String s, Exception nestedException) {
        super(s);
        this.nestedException = nestedException;
    }

    /**
     * Creates a JobException with a message, a nested exception, and a flag concerning
     * further schedules.
     *
     * @param s                      Message
     * @param nestedException        nested exception
     * @param furtherScheduleAllowed states whether further schedules are allowed or not
     */
    public JobException(String s, Exception nestedException, boolean furtherScheduleAllowed) {
        this(s, nestedException);
        this.furtherScheduleAllowed = furtherScheduleAllowed;
    }

    /**
     * Returns the nested exception.
     *
     * @return nested exception
     */
    public Exception getNestedException() {
        return nestedException;
    }

    /**
     * Returns whether further schedules are allowed or not.
     *
     * @return true/false
     */
    public boolean isFurtherScheduleAllowed() {
        return furtherScheduleAllowed;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[JobException, message=");
        b.append(getMessage());
        b.append(", furtherScheduleAllowed=");
        b.append(furtherScheduleAllowed);
        if (nestedException != null) {
            b.append(", nestedException=");
            b.append(nestedException.toString());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            nestedException.printStackTrace(pw);
            b.append(", stackTrace=");
            pw.flush();
            b.append(sw.toString());
            pw.close();
        }
        b.append("]");
        return b.toString();
    }
}
