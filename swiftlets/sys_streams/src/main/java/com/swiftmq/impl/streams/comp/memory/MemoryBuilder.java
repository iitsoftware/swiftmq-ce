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

package com.swiftmq.impl.streams.comp.memory;

import com.swiftmq.impl.streams.StreamContext;

/**
 * Factory to create Memories.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class MemoryBuilder {
    StreamContext ctx;
    String name;

    /**
     * Internal use.
     */
    public MemoryBuilder(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    /**
     * Creates a Memory that stores Messages on the Heap
     *
     * @return HeapMemory
     */
    public HeapMemory heap() {
        return (HeapMemory) ctx.stream.addMemory(name, new HeapMemory(ctx, name));
    }

    /**
     * Creates a Memory that stores Messages in a regular Queue
     *
     * @param queueName Queue that serves as a persistent store
     * @return QueueMemory
     */
    public QueueMemory queue(String queueName) throws Exception {
        return (QueueMemory) ctx.stream.addMemory(name, new QueueMemory(ctx, name, queueName));
    }

    /**
     * Creates a Memory that stores Messages in a regular Queue and shared that Queue with other
     * QueueMemories of the same Stream
     *
     * @param queueName Queue that serves as a shared persistent store
     * @return QueueMemory
     */
    public QueueMemory sharedQueue(String queueName) throws Exception {
        return (QueueMemory) ctx.stream.addMemory(name, new QueueMemory(ctx, name, queueName).shared());
    }

    /**
     * Creates a Memory that stores Messages in a Temporary Queue
     *
     * @return TempQueueMemory
     */
    public TempQueueMemory tempqueue() throws Exception {
        return (TempQueueMemory) ctx.stream.addMemory(name, new TempQueueMemory(ctx, name));
    }
}
