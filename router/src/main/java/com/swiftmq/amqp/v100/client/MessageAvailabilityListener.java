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

package com.swiftmq.amqp.v100.client;

/**
 * <p>A listener that can be registered on a Consumer with a receiveNoWait call to get a notification when new messages become available.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 */

public interface MessageAvailabilityListener {
    /**
     * Will be called from a Consumer when a message becomes available. The Consumer is locked during the call so never
     * access the Consumer out of this method call (rather the intention is to notify another thread to call receive on it)
     *
     * @param consumer The calling Consumer
     */
    public void messageAvailable(Consumer consumer);
}
