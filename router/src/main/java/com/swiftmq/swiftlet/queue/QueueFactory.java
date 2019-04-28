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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.mgmt.Entity;

/**
 * Interface for queue factories.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface QueueFactory {

    /**
     * Returns whether queues created from this factory should be registered in the Usage section of the management tree
     *
     * @return true/false
     */
    public boolean registerUsage();

    /**
     * Creates a new abstract queue from the given entity.
     *
     * @param queueName   queue name
     * @param queueEntity queue entity
     * @throws QueueException on error creating the queue
     */
    public AbstractQueue createQueue(String queueName, Entity queueEntity)
            throws QueueException;
}

