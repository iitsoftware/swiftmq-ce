/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.streams.processor;

import com.swiftmq.impl.streams.SwiftletContext;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.collection.ConcurrentExpandableList;
import com.swiftmq.tools.collection.ExpandableList;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class EventLoopMUX {
    private SwiftletContext ctx;
    private final EventLoop[] eventLoops;
    private final ExpandableList<Pair> registrations = new ConcurrentExpandableList<>();
    private final AtomicInteger nextLoop = new AtomicInteger();

    public EventLoopMUX(SwiftletContext ctx, String id, int numberLoops) {
        this.ctx = ctx;
        this.eventLoops = new EventLoop[numberLoops];
        for (int i = 0; i < numberLoops; i++)
            eventLoops[i] = ctx.threadpoolSwiftlet.createEventLoop(id, new ProcessorProxy());
    }

    private int getNextLoop() {
        while (true) {
            int current = nextLoop.get();
            int next = (current + 1) % eventLoops.length;
            if (nextLoop.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    public int register(MUXProcessor muxProcessor) {
        return registrations.add(new Pair(muxProcessor, eventLoops[getNextLoop()]));
    }

    public void unregister(int id) {
        registrations.remove(id);
    }

    public void submit(int id, Object event) {
        Pair pair = registrations.get(id);
        if (pair != null)
            pair.eventLoop.submit(new EventWrapper(id, event));
    }

    public void close() {
        for (EventLoop eventLoop : eventLoops) {
            eventLoop.close();
        }
    }

    private static class Pair {
        MUXProcessor muxProcessor;
        EventLoop eventLoop;

        public Pair(MUXProcessor muxProcessor, EventLoop eventLoop) {
            this.muxProcessor = muxProcessor;
            this.eventLoop = eventLoop;
        }
    }

    private final class ProcessorProxy implements EventProcessor {
        @Override
        public void process(List<Object> list) {
            list.forEach(w -> {
                EventWrapper wrapper = (EventWrapper) w;
                Pair pair = registrations.get(wrapper.id);
                if (pair != null) {
                    pair.muxProcessor.process(wrapper.event);
                }
            });
        }
    }

    private static class EventWrapper {
        int id;
        Object event;

        public EventWrapper(int id, Object event) {
            this.id = id;
            this.event = event;
        }
    }
}
