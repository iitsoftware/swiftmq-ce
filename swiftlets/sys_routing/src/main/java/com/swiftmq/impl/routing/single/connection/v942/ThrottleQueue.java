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

package com.swiftmq.impl.routing.single.connection.v942;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory;
import com.swiftmq.impl.routing.single.smqpr.v942.ThrottleRequest;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.requestreply.Request;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThrottleQueue {
    SwiftletContext ctx;
    RoutingConnection connection;
    final AtomicBoolean closed = new AtomicBoolean(false);
    Semaphore sem = null;
    EventLoop eventLoop;
    public ThrottleQueue(SwiftletContext ctx, RoutingConnection connection) {
        this.ctx = ctx;
        this.connection = connection;
        sem = new Semaphore();
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$routing.connection.throttle", new EventProcessor() {
            @Override
            public void process(List<Object> list) {
                for (Object event : list) {
                    if (closed.get())
                        break;
                    Request r = (Request) event;
                    if (r.getDumpId() == SMQRFactory.THROTTLE_REQ) {
                        sem.reset();
                        sem.waitHere(((ThrottleRequest) r).getDelay());
                    } else {
                        connection.getOutboundQueue().submit(r);
                    }
                }
            }
        });
    }

    public void enqueue(Object event) {
        eventLoop.submit(event);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        sem.notifySingleWaiter();
        eventLoop.close();
    }
}
