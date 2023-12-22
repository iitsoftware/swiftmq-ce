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

package com.swiftmq.impl.routing.single.connection;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.smqpr.BulkRequest;
import com.swiftmq.impl.routing.single.smqpr.KeepAliveRequest;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataStreamOutputStream;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

public class OutboundWriter
        implements TimerListener {
    static KeepAliveRequest keepAliveRequest = new KeepAliveRequest();
    SwiftletContext ctx = null;
    RoutingConnection routingConnection = null;
    Connection connection = null;
    DataStreamOutputStream outStream;
    final AtomicBoolean closed = new AtomicBoolean(false);
    TraceSpace traceSpace = null;
    boolean compression = false;
    DataByteArrayOutputStream bos = new DataByteArrayOutputStream();
    DataByteArrayOutputStream bos2 = new DataByteArrayOutputStream();
    EventLoop eventLoop;

    OutboundWriter(SwiftletContext ctx, RoutingConnection routingConnection, boolean compression) throws IOException {
        this.ctx = ctx;
        this.routingConnection = routingConnection;
        this.connection = routingConnection.getConnection();
        this.compression = compression;
        this.outStream = new DataStreamOutputStream(connection.getOutputStream());
        traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$routing.connection.service", new EventProcessor() {
            final BulkRequest bulkRequest = new BulkRequest();

            @Override
            public void process(List<Object> list) {
                if (list.size() == 1)
                    writeObject((Dumpable) list.get(0));
                else {
                    bulkRequest.dumpables = list.toArray();
                    bulkRequest.len = list.size();
                    writeObject(bulkRequest);
                    bulkRequest.dumpables = null;
                    bulkRequest.len = 0;
                }
            }
        });
    }

    private void compress(byte[] data, int len) throws IOException {
        GZIPOutputStream gos = new GZIPOutputStream(bos2, len);
        gos.write(data, 0, len);
        gos.finish();
    }

    public void writeObject(Dumpable obj) {
        if (closed.get())
            return;
        if (traceSpace.enabled) traceSpace.trace("smqr", toString() + ": write object: " + obj);
        try {
            if (compression) {
                Dumpalizer.dump(bos, obj);
                compress(bos.getBuffer(), bos.getCount());
                outStream.writeInt(bos2.getCount());
                outStream.write(bos2.getBuffer(), 0, bos2.getCount());
                bos.rewind();
                bos2.rewind();
            } else
                Dumpalizer.dump(outStream, obj);
            outStream.flush();
        } catch (Exception e) {
            if (traceSpace.enabled) traceSpace.trace("smqr", toString() + ": exception write object, exiting!: " + e);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(connection); // closes the connection
            eventLoop.close();
            closed.set(true);
        }
    }

    public EventLoop getOutboundQueue() {
        return eventLoop;
    }

    public void performTimeAction() {
        eventLoop.submit(keepAliveRequest);
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        eventLoop.close();
    }

    public String toString() {
        return routingConnection.toString() + "/OutboundWriter";
    }
}

