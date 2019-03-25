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
 * A JobGroup is the interface between the Scheduler Swiftlet and other Swiftlets.
 * Each Swiftlet should have their own unique JobGroup to register its JobFactories.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public interface JobGroup {
    /**
     * Returns the name of the JobGroup. This name should be unique and should reflect
     * the Swiftlet in some manner, e.g. 'Store' for the StoreSwiftlet. The name is set
     * during creation of the JobGroup.
     *
     * @return Name
     */
    public String getName();

    /**
     * Returns an array of registered JobFactory names. If no JobFactories are registered,
     * null is returned.
     *
     * @return JobFactory names
     */
    public String[] getJobFactoryNames();

    /**
     * Checks whether a JobFactory with a given name is registered at this JobGroup.
     *
     * @param name JobFactory name
     * @return true/false
     */
    public boolean hasJobFactory(String name);

    /**
     * Registers a JobFactory at this JobGroup. Schedules which refers to this JobFactory are activated.
     *
     * @param name    Name of the JobFactory
     * @param factory JobFactory
     */
    public void addJobFactory(String name, JobFactory factory);

    /**
     * Removes a JobFactory from this JobGroup. Schedules which refers to this JobFactory are deactivated,
     * running Jobs from this JobFactory are stopped.
     *
     * @param name Name of the JobFactory
     */
    public void removeJobFactory(String name);

    /**
     * Removes all JobFactories from this JobGroup. Schedules which refers to this JobFactories are deactivated,
     * running Jobs from this JobFactories are stopped.
     */
    public void removeAll();
}
