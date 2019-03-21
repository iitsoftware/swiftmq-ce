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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.comp.message.Message;

/**
 * Base interface for Inputs.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public interface Input {
    /**
     * Returns the name of this Input.
     *
     * @return Name
     */
    public String getName();

    /**
     * Sets the current Message on this Input.
     *
     * @param current Message
     * @return Input
     */
    public Input current(Message current);

    /**
     * Returns the current Message of this Input.
     *
     * @return Message
     */
    public Message current();

    /**
     * Internal use.
     */
    public void executeCallback() throws Exception;


    /**
     * Internal use.
     */
    public void collect(long interval);

    /**
     * Starts this Input. This method is called automatically if an Input is created
     * outside a callback. If it is created inside, it must be called explicitly.
     */
    public void start() throws Exception;

    /**
     * Closes this Input.
     */
    public void close();

}
