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

package com.swiftmq.impl.net.standard.scheduler;

import com.swiftmq.impl.net.standard.CountableBufferedInputStream;
import com.swiftmq.impl.net.standard.CountableWrappedOutputStream;
import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IntraVMServerEndpointImpl extends Connection
        implements IntraVMServerEndpoint {
    NetworkSwiftlet networkSwiftlet = null;
    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    ConnectionManager connectionManager = null;
    IntraVMConnection clientConnection = null;
    DataByteArrayInputStream dis = null;
    DataByteArrayOutputStream dos = null;
    InputStream in = null;
    OutputStream out = null;
    InboundHandler inboundHandler = null;

    public IntraVMServerEndpointImpl(IntraVMConnection clientConnection) {
        super(false);
        this.clientConnection = clientConnection;
        init();
        clientConnection.setEndpoint(this);
    }

    private void init() {
        networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        connectionManager = networkSwiftlet.getConnectionManager();
        dis = new DataByteArrayInputStream();
        in = new CountableBufferedInputStream(dis);
        dos = new DataByteArrayOutputStream() {
            public void flush() throws IOException {
                if (isClosed() || clientConnection.isClosed())
                    throw new IOException("Connection is closed");
                super.flush();
                clientConnection.chunkCompleted(getBuffer(), 0, getCount());
                rewind();
            }
        };
        out = new CountableWrappedOutputStream(dos);
    }

    public void setInboundHandler(InboundHandler handler) {
        super.setInboundHandler(handler);
        inboundHandler = handler;
    }

    // --> ChunkListener
    public void chunkCompleted(byte[] b, int offset, int len) {
        dis.setBuffer(b, offset, len);
        try {
            inboundHandler.dataAvailable(this, in);
        } catch (IOException e) {
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/Exception, EXITING: " + e);
            logSwiftlet.logInformation(toString(), "Exception, EXITING: " + e);
            if (!isClosed()) {
                connectionManager.removeConnection(this);
            }
        }
    }
    // <-- ChunkListener

    // --> IntraVMServerEndpoint
    public void clientClose() {
        connectionManager.removeConnection(this);
    }
    // <-- IntraVMServerEndpoint

    // --> Connection
    public String getHostname() {
        return clientConnection.getLocalHostname();
    }

    public InputStream getInputStream() {
        return in;
    }

    public OutputStream getOutputStream() {
        return out;
    }

    public synchronized void close() {
        if (isClosed())
            return;
        super.close();
        clientConnection.serverClose();
    }
    // <-- Connection

    public String toString() {
        return clientConnection.getLocalHostname();
    }
}
