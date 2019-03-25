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

import java.util.Map;

/**
 * A JobFactory is registered at a JobGroup and creates Jobs. The name of the factory
 * should be the name of the Job, created from this factory. For example, a factory creating
 * 'Queue Mover' jobs returns 'Queue Mover' as its name.
 * <br><br>
 * Before a Job is obtained from the factory with "getJobInstance", the job parameters are
 * retrieved and verified. If that was ok, the Job is obtained from the factory via the
 * "getJobInstance" method. After the Job has been finished, the method "finished" is
 * called with the Job instance and eventually a JobException, thrown by the Job.
 * <br><br>
 * Due to this check-out (getJobInstance) and check-in (finished) behavior, a JobFactory
 * can be implemented as a pool of Jobs with pre-created Job instances etc.
 * <br><br>
 * A JobFactory is accessed by a single thread at a time, so there is no need for synchronization.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public interface JobFactory {
    /**
     * Returns the name of the JobFactory, e.g. 'Queue Mover'
     *
     * @return Name
     */
    public String getName();

    /**
     * Returns a short description of the JobFactory, e.g. 'Moves the content of a Queue'
     *
     * @return description
     */
    public String getDescription();

    /**
     * Returns a Map of JobParameters, keyed by the parameter name or null if no parameter
     * is defined.
     *
     * @return Map of JobParameters
     */
    public Map getJobParameters();

    /**
     * Returns a JobParameter with the given parameter name.
     *
     * @param name Parameter name
     * @return JobParameter or null
     */
    public JobParameter getJobParameter(String name);

    /**
     * Returns a Job instance. This is equal to a check-out of a pool.
     *
     * @return Job
     */
    public Job getJobInstance();

    /**
     * Marks a Job instance as finished. Called after the Job has been terminated. This is
     * equal to a check-in into a pool.
     *
     * @param job       Job instance
     * @param exception JobException thrown by the Job instance
     */
    public void finished(Job job, JobException exception);
}
