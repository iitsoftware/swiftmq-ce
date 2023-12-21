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

package com.swiftmq.impl.amqp;

import com.swiftmq.amqp.Writable;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.tools.util.DataStreamOutputStream;

import java.util.List;

public class OutboundQueue implements EventProcessor {
    SwiftletContext ctx;
    VersionedConnection connection;
    DataStreamOutputStream dos = null;
    OutboundTracer outboundTracer = null;
    EventLoop eventLoop;

    OutboundQueue(SwiftletContext ctx, VersionedConnection connection) {
        this.ctx = ctx;
        this.eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$amqp.connection.service", this);
        this.connection = connection;
        dos = new DataStreamOutputStream(connection.getConnection().getOutputStream());
    }

    public void setOutboundTracer(OutboundTracer outboundTracer) {
        this.outboundTracer = outboundTracer;
    }

    public void enqueue(Object event) {
        eventLoop.submit(event);
    }

    @Override
    public void process(List<Object> list) {
        try {
            for (Object event : list) {
                ((Writable) event).writeContent(dos);
                if (outboundTracer != null && ctx.protSpace.enabled)
                    ctx.protSpace.trace(outboundTracer.getTraceKey(), connection.toString() + "/SND: " + outboundTracer.getTraceString(event));
            }
            dos.flush();
        } catch (Exception e) {
            connection.close();
        } finally {
            for (Object event : list) {
                Writable w = (Writable) event;
                if (w.getSemaphore() != null)
                    w.getSemaphore().notifySingleWaiter();
                else if (w.getCallback() != null)
                    w.getCallback().done(true);
            }
        }
    }

    public void close() {
        eventLoop.close();
    }
}
