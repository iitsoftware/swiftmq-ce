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

import javax.jms.Destination;

/**
 * Factory to create Outputs.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class OutputBuilder {
    StreamContext ctx;
    String name;

    /**
     * Internal use.
     */
    public OutputBuilder(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    /**
     * Creates a new QueueOutput.
     *
     * @return QueueOutput
     */
    public QueueOutput queue() throws Exception {
        if (name != null)
            return (QueueOutput) ctx.stream.addOutput(name, new QueueOutput(ctx, name));
        return new QueueOutput(ctx, name);
    }

    /**
     * Creates a new TopicOutput.
     *
     * @return TopicOutput
     */
    public TopicOutput topic() throws Exception {
        if (name != null)
            return (TopicOutput) ctx.stream.addOutput(name, new TopicOutput(ctx, name));
        return new TopicOutput(ctx, name);
    }

    /**
     * Verifies the type of the address (Topic/Queue) and returns the corresponding Output.
     *
     * @param address Address
     * @return Output
     * @throws Exception
     */
    public Output forAddress(Destination address) throws Exception
    {
        Output output = new QueueOutput(ctx, name);
        if (name != null)
            ctx.stream.addOutput(name, output);
        output.setDestination(address);
        return output;
    }
}
