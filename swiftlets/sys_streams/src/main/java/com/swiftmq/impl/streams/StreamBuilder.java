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

package com.swiftmq.impl.streams;

import com.swiftmq.impl.streams.comp.io.InputBuilder;
import com.swiftmq.impl.streams.comp.io.MailServer;
import com.swiftmq.impl.streams.comp.io.OutputBuilder;
import com.swiftmq.impl.streams.comp.io.TempQueue;
import com.swiftmq.impl.streams.comp.jdbc.JDBCLookup;
import com.swiftmq.impl.streams.comp.memory.MemoryBuilder;
import com.swiftmq.impl.streams.comp.memory.MemoryGroup;
import com.swiftmq.impl.streams.comp.message.MessageBuilder;
import com.swiftmq.impl.streams.comp.timer.TimerBuilder;

/**
 * Factory to create Stream resources.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */

public class StreamBuilder {
    StreamContext ctx;

    /**
     * Internal use only.
     */
    StreamBuilder(StreamContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Returns a new MemoryBuilder
     *
     * @param name of the Memory
     * @return MemoryBuilder
     */
    public MemoryBuilder memory(String name) {
        return new MemoryBuilder(ctx, name);
    }

    /**
     * Creates a new MemoryGroup
     *
     * @param name              Name of the MemoryGroup
     * @param groupPropertyName Name of the Group Property
     * @return MemoryGroup
     */
    public MemoryGroup memoryGroup(String name, String groupPropertyName) {
        return ctx.stream.addMemoryGroup(name, new MemoryGroup(ctx, name, groupPropertyName));
    }

    /**
     * Returns a new TimerBuilder
     *
     * @param name of the Timer
     * @return TimerBuilder
     */
    public TimerBuilder timer(String name) {
        return new TimerBuilder(ctx, name);
    }

    /**
     * Returns a new OutputBuilder. If the name parameter is set to null, the Output will
     * not be registered at the stream and no usage information is generated. This is useful
     * for Outputs that are used to send a single message only (e.g. sending a reply in
     * request/reply). These Outputs needs to be closed explicitly.
     *
     * @param name of the Output
     * @return OutputBuilder
     */
    public OutputBuilder output(String name) {
        return new OutputBuilder(ctx, name);
    }

    /**
     * Returns a new InputBuilder
     *
     * @param name of the Input
     * @return InputBuilder
     */
    public InputBuilder input(String name) {
        return new InputBuilder(ctx, name);
    }

    /**
     * Returns a new InputBuilder
     *
     * @param tempQueue temp queue
     * @return InputBuilder
     */
    public InputBuilder input(TempQueue tempQueue) {
        return new InputBuilder(ctx, tempQueue.queueName());
    }

    /**
     * Returns a new MessageBuilder
     *
     * @return MessageBuilder
     */
    public MessageBuilder message() {
        return new MessageBuilder(ctx);
    }

    /**
     * Returns a new MailServer
     *
     * @param hostname Host Name
     * @return MailServer
     */
    public MailServer mailserver(String hostname) {
        return ctx.stream.addMailServer(hostname, new MailServer(ctx, hostname));
    }

    /**
     * Returns a new JDBCLookup
     *
     * @param name of the JDBCLookup
     * @return JDBCLookup
     */
    public JDBCLookup jdbcLookup(String name) {
        return ctx.stream.addJDBCLookup(name, new JDBCLookup(ctx, name));
    }

    /**
     * Returns a new TempQueue
     *
     * @param name of the TempQueue
     * @return TempQueue
     */
    public TempQueue tempQueue(String name) throws Exception {
        return ctx.stream.addTempQueue(name, new TempQueue(ctx, name));
    }

}
