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
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataStreamOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class OutboundWriter
        implements TimerListener {
    static final String TP_CONNSVC = "sys$routing.connection.service";
    static KeepAliveRequest keepAliveRequest = new KeepAliveRequest();
    SwiftletContext ctx = null;
    ThreadPool myTP = null;
    RoutingConnection routingConnection = null;
    Connection connection = null;
    DataStreamOutputStream outStream;
    OutboundQueue outboundQueue = null;
    OutboundProcessor outboundProcessor = null;
    boolean closed = false;
    TraceSpace traceSpace = null;
    boolean compression = false;
    DataByteArrayOutputStream bos = new DataByteArrayOutputStream();
    DataByteArrayOutputStream bos2 = new DataByteArrayOutputStream();

    OutboundWriter(SwiftletContext ctx, RoutingConnection routingConnection, boolean compression) throws IOException {
        this.ctx = ctx;
        this.routingConnection = routingConnection;
        this.connection = routingConnection.getConnection();
        this.compression = compression;
        this.outStream = new DataStreamOutputStream(connection.getOutputStream());
        traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
        myTP = ctx.threadpoolSwiftlet.getPool(TP_CONNSVC);
        outboundProcessor = new OutboundProcessor();
        outboundQueue = new OutboundQueue();
        outboundQueue.startQueue();
    }

    private void compress(byte[] data, int len) throws IOException {
        GZIPOutputStream gos = new GZIPOutputStream(bos2, len);
        BufferedOutputStream dos = new BufferedOutputStream(gos, len);
        gos.write(data, 0, len);
        gos.finish();
    }

    public void writeObject(Dumpable obj) {
        if (closed)
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
            outboundQueue.close();
            closed = true;
        }
    }

    public SingleProcessorQueue getOutboundQueue() {
        return outboundQueue;
    }

    public void performTimeAction() {
        outboundQueue.enqueue(keepAliveRequest);
    }

    public void close() {
        outboundQueue.close();
        closed = true;
    }

    public String toString() {
        return routingConnection.toString() + "/OutboundWriter";
    }

    private class OutboundQueue extends SingleProcessorQueue {
        BulkRequest bulkRequest = new BulkRequest();

        OutboundQueue() {
            super(100);
        }

        protected void startProcessor() {
            myTP.dispatchTask(outboundProcessor);
        }

        protected void process(Object[] bulk, int n) {
            if (n == 1)
                writeObject((Dumpable) bulk[0]);
            else {
                bulkRequest.dumpables = bulk;
                bulkRequest.len = n;
                writeObject(bulkRequest);
                bulkRequest.dumpables = null;
                bulkRequest.len = 0;
            }
        }
    }

    private class OutboundProcessor implements AsyncTask {

        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return TP_CONNSVC;
        }

        public String getDescription() {
            return ctx.routingSwiftlet.getName() + "/" + connection.toString() + "/OutboundProcessor";
        }

        public void stop() {
        }

        public void run() {
            if (outboundQueue.dequeue() && !closed)
                myTP.dispatchTask(this);
        }
    }
}

