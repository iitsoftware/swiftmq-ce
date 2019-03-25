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

import com.swiftmq.swiftlet.timer.event.TimerListener;

/**
 * Wrapper for an active (started) queue.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class ActiveQueue implements TimerListener {
    transient AbstractQueue abstractQueue;
    long startupTime = -1;


    /**
     * Constructs an ActiveQueue.
     *
     * @param abstractQueue queue.
     */
    public ActiveQueue(AbstractQueue abstractQueue) {
        // SBgen: Assign variable
        this.abstractQueue = abstractQueue;
    }

    /**
     * Returns the queue.
     *
     * @return queue.
     */
    public AbstractQueue getAbstractQueue() {
        // SBgen: Get variable
        return (abstractQueue);
    }

    /**
     * Returns the startup time.
     *
     * @return startup time.
     */
    public long getStartupTime() {
        // SBgen: Get variable
        return (startupTime);
    }

    /**
     * Sets the startup time.
     *
     * @param startupTime startup time.
     */
    public void setStartupTime(long startupTime) {
        // SBgen: Assign variable
        this.startupTime = startupTime;
    }

    /**
     * Cleanup expired messages.
     * Implemenation of TimerListener
     */
    public void performTimeAction() {
        try {
            abstractQueue.cleanUpExpiredMessages();
        } catch (QueueException e) {
            // Log to error log
        }
    }
}

