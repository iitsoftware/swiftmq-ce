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

import com.swiftmq.impl.streams.StreamContext;

/**
 * Factory to create Inputs.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class InputBuilder {
    StreamContext ctx;
    String name;
    TempQueue tempQueue;

    /**
     * Internal use.
     */
    public InputBuilder(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    /**
     * Creates a new QueueInput.
     *
     * @return QueueInput
     */
    public QueueInput queue() throws Exception {
        return (QueueInput) ctx.stream.addInput(name, new QueueInput(ctx, name));
    }

    /**
     * Creates a new QueueWireTapInput.
     *
     * @return QueueWireTapInput
     */
    public QueueWireTapInput wiretap(String wiretapName) throws Exception {
        return (QueueWireTapInput) ctx.stream.addInput(wiretapName, new QueueWireTapInput(ctx, name, wiretapName));
    }

    /**
     * Creates a new TopicInput.
     *
     * @return TopicInput
     */
    public TopicInput topic() throws Exception {
        return (TopicInput) ctx.stream.addInput(name, new TopicInput(ctx, name));
    }

    /**
     * Creates a new ManagementInput and registers it under the context name.
     *
     * @return ManagementInput
     */
    public ManagementInput management() throws Exception {
        return (ManagementInput) ctx.stream.addInput(name, new ManagementInput(ctx, name));
    }

    /**
     * Creates a new ManagementInput and registers it under a different name.
     *
     * @param registrationName
     * @return ManagementInput
     */
    public ManagementInput management(String registrationName) throws Exception {
        return (ManagementInput) ctx.stream.addInput(registrationName, new ManagementInput(ctx, name));
    }

}
