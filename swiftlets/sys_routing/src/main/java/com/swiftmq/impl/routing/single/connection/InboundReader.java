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
import com.swiftmq.impl.routing.single.smqpr.SMQRFactory;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestServiceRegistry;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataStreamInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class InboundReader extends RequestServiceRegistry
        implements InboundHandler, TimerListener {
    SwiftletContext ctx = null;
    RoutingConnection routingConnection = null;
    Connection connection = null;
    DumpableFactory dumpableFactory = null;
    DataStreamInputStream dis = new DataStreamInputStream();
    DataByteArrayInputStream dbis = new DataByteArrayInputStream();
    DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    TraceSpace traceSpace = null;
    volatile int keepaliveCount = 5;
    boolean compression = false;

    InboundReader(SwiftletContext ctx, RoutingConnection routingConnection, DumpableFactory dumpableFactory, boolean compression) {
        this.ctx = ctx;
        this.routingConnection = routingConnection;
        this.connection = routingConnection.getConnection();
        this.dumpableFactory = dumpableFactory;
        this.compression = compression;
        traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_PROTOCOL);
    }

    public void performTimeAction() {
        keepaliveCount--;
        if (traceSpace.enabled)
            traceSpace.trace("smqr", toString() + ": decrementing keepaliveCount to: " + keepaliveCount);
        if (keepaliveCount == 0) {
            if (traceSpace.enabled) traceSpace.trace("smqr", toString() + ": keepalive counter reaching 0, exiting!");
            ctx.logSwiftlet.logWarning("smqr", toString() + ": keepalive counter reaching 0, exiting!");
            ctx.networkSwiftlet.getConnectionManager().removeConnection(connection); // closes the connection
        }
    }

    private void decompress() throws IOException {
        GZIPInputStream gis = new GZIPInputStream(dbis);
        int len;
        while ((len = gis.read(buffer, 0, buffer.length)) != -1)
            dbos.write(buffer, 0, len);
    }

    public void dataAvailable(Connection c, InputStream inputStream)
            throws IOException {
        if (compression) {
            dbis.reset();
            dbos.rewind();
            dis.setInputStream(inputStream);
            int len = dis.readInt();
            byte b[] = new byte[len];
            dis.readFully(b);
            dbis.setBuffer(b);
            decompress();
            dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
            dis.setInputStream(dbis);
        } else
            dis.setInputStream(inputStream);
        Dumpable obj = Dumpalizer.construct(dis, dumpableFactory);
        if (traceSpace.enabled) traceSpace.trace("smqr", toString() + ": read object: " + obj);
        if (obj.getDumpId() != SMQRFactory.KEEPALIVE_REQ) {
            if (obj.getDumpId() == SMQRFactory.BULK_REQ) {
                BulkRequest bulkRequest = (BulkRequest) obj;
                for (int i = 0; i < bulkRequest.len; i++) {
                    Request req = (Request) bulkRequest.dumpables[i];
                    if (req.getDumpId() != SMQRFactory.KEEPALIVE_REQ) {
                        dispatch(req);
                    } else {
                        keepaliveCount++;
                        if (traceSpace.enabled)
                            traceSpace.trace("smqr", toString() + ": incrementing keepaliveCount to: " + keepaliveCount);
                    }
                }
            } else {
                dispatch((Request) obj);
            }
        } else {
            keepaliveCount = 5;
            if (traceSpace.enabled)
                traceSpace.trace("smqr", toString() + ": setting keepaliveCount to: " + keepaliveCount);
        }
    }

    public String toString() {
        return routingConnection.toString() + "/InboundReader";
    }
}

