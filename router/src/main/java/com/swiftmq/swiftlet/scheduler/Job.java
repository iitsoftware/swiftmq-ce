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

import java.util.Properties;

/**
 * A Job is created from a JobFactory and performs a task.
 * <br><br>
 * After creation and parameter verification, the start method is called. Parameter
 * values are passed as String objects. It will be checked from the Scheduler Swiftlet
 * whether all mandatory parameters have been set before the start method is called. These
 * parameters can also be verified by an optional JobParameterVerifier. In any case,
 * the Properties object passed contains all mandatory parameter values. The second
 * parameter is a JobTerminationListener to be called when the Job terminates.
 * <br><br>
 * A Job should be interruptable, that is, it should terminate if the stop method is
 * called from the Scheduler Swiftlet. This method is called either when the Job reaches
 * its maximum runtime, configured by the user, or when it should terminate for Scheduler
 * internal reasons, e.g. a re-schedule, or during shutdown of the router. The stop method
 * is called asynchronously.
 * <br><br>
 * A Job is marked as running by calling its start method. The start method
 * might block until the Job has been finished or it might return immediately, e.g.
 * if the task is performed by another thread.
 * <br><br>
 * A Job is marked as finished after the JobTerminationListener has been called or
 * after the Scheduler Swiftlet has called the stop method of the Job, respectively.
 * From the perspective of the Job instance that means it has to call the JobTerminationListener
 * after it has finished his work, except the stop method has been called in the meantime.
 * <br><br>
 * A finished Job will be re-scheduled.
 * <br><br>
 * A JobException can be thrown by the start and stop method and it can be passed to the
 * JobTerminationListener. A JobException has an attribute which states whether further
 * schedules are allowed or not. If not, the Job is marked as errorneous and a user
 * intervention is acquired. Otherwise, the Job is re-scheduled.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public interface Job {
    /**
     * Starts a Job. This method might block until the Job finished or it might return immediately.
     * The Job is marked as running until the Job calls the JobTerminationListener
     * or the Scheduler Swiftlet calles the stop method.
     *
     * @param parameters parameter values
     * @param listener   JobTerminationListener
     * @throws JobException on error
     */
    public void start(Properties parameters, JobTerminationListener listener) throws JobException;

    /**
     * Stops a Job asynchronously. The Job is marked as terminated thereafter. This method must
     * not block. The stop method is called when the maximum runtime for the Job is reached or
     * for Scheduler Swiftlet internal reasons, e.g. a re-schedule or a router shutdown.
     *
     * @throws JobException on error
     */
    public void stop() throws JobException;
}
