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

package com.swiftmq.net.client;

import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.swiftlet.net.IntraVMServerEndpoint;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class IntraVMConnection implements Connection, ChunkListener {
    static int connectionId = 0;

    IntraVMServerEndpoint endpoint = null;
    InboundHandler inboundHandler = null;
    ExceptionHandler exceptionHandler = null;
    DataByteArrayInputStream dis = null;
    OutputStream out = null;
    String myHostname = null;
    boolean closed = false;
    AtomicBoolean inputActiveIndicator = null;

    public IntraVMConnection() {
        dis = new DataByteArrayInputStream();
        out = new DataByteArrayOutputStream() {
            public void flush() throws IOException {
                if (closed || endpoint.isClosed())
                    throw new IOException("Connection is closed");
                super.flush();
                endpoint.chunkCompleted(getBuffer(), 0, getCount());
                rewind();
            }
        };
        myHostname = "INTRAVM-" + getConnectionId();
    }

    public void setEndpoint(IntraVMServerEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setInputActiveIndicator(AtomicBoolean inputActiveIndicator) {
        this.inputActiveIndicator = inputActiveIndicator;
    }

    private static synchronized int getConnectionId() {
        return connectionId++;
    }

    public void chunkCompleted(byte[] b, int offset, int len) {
        dis.setBuffer(b, offset, len);
        inboundHandler.dataAvailable(dis);
    }

    public void setInboundHandler(InboundHandler inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public OutputStream getOutputStream() {
        return out;
    }

    public String getLocalHostname() {
        return myHostname;
    }

    public String getHostname() {
        return "INTRAVM";
    }

    public int getPort() {
        return 0;
    }

    public void start() {
    }

    public boolean isClosed() {
        return closed;
    }

    public void serverClose() {
        if (exceptionHandler != null)
            exceptionHandler.onException(new IOException("Server closed connection!"));
        closed = true;
    }

    public void close() {
        endpoint.clientClose();
        closed = true;
    }
}
