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

package com.swiftmq.swiftlet.queue.event;

import com.swiftmq.swiftlet.queue.QueueManager;

import java.util.EventObject;

/**
 * An event fired by the QueueManager
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class QueueManagerEvent extends EventObject {
    String queueName;

    /**
     * Constructs a new QueueManagerEvent
     *
     * @param queueName    The queue name
     * @param queueManager The queue manager
     * @SBGen Constructor
     */
    public QueueManagerEvent(QueueManager queueManager, String queueName) {
        super(queueManager);
        this.queueName = queueName;
    }

    /**
     * Returns the queue name
     *
     * @return queue name
     * @SBGen Method get queueName
     */
    public String getQueueName() {
        // SBgen: Get variable
        return (queueName);
    }
}

