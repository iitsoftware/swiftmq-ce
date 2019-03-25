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

import java.util.EventListener;

/**
 * A listener that is interested in events from the QueueManager
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface QueueManagerListener extends EventListener {
    /**
     * Will be called from the QueueManager before queue start.
     *
     * @param evt queue manager event
     */
    public void queueStartInitiated(QueueManagerEvent evt);

    /**
     * Will be called from the QueueManager after queue start.
     *
     * @param evt queue manager event
     */
    public void queueStarted(QueueManagerEvent evt);

    /**
     * Will be called from the QueueManager before queue stop.
     *
     * @param evt queue manager event
     */
    public void queueStopInitiated(QueueManagerEvent evt);

    /**
     * Will be called from the QueueManager after queue stop.
     *
     * @param evt queue manager event
     */
    public void queueStopped(QueueManagerEvent evt);
}

