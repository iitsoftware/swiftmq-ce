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

import com.swiftmq.impl.net.standard.NetworkSwiftletImpl;
import com.swiftmq.impl.net.standard.TCPConnection;
import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingHandler implements AsyncTask, ChunkListener, TimerListener {
    NetworkSwiftletImpl networkSwiftlet = null;
    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    TimerSwiftlet timerSwiftlet = null;
    ConnectionManager connectionManager = null;
    ProtocolInputHandler inputHandler = null;
    InboundHandler inboundHandler = null;
    DataByteArrayInputStream bais = null;
    boolean closed = false;
    volatile boolean zombi = true;

    TCPConnection connection = null;
    AtomicBoolean inputActiveIndicator = null;

    BlockingHandler(TCPConnection connection) {
        this.connection = connection;
        inputActiveIndicator = connection.getInputActiveIndicator();
        connection.setHandler(this);
        int size = 0;
        try {
            size = connection.getSocket().getReceiveBufferSize();
        } catch (SocketException e) {
        }
        networkSwiftlet = (NetworkSwiftletImpl) SwiftletManager.getInstance().getSwiftlet("sys$net");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        connectionManager = networkSwiftlet.getConnectionManager();
        inputHandler = connection.getMetaData().createProtocolInputHandler();
        connection.setProtocolInputHandler(inputHandler);
        inputHandler.setChunkListener(this);
        inputHandler.createInputBuffer(connection.getMetaData().getInputBufferSize(), connection.getMetaData().getInputExtendSize());
        inboundHandler = connection.getInboundHandler();
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        long zombiConnectionTimeout = networkSwiftlet.getZombiConnectionTimeout();
        if (zombiConnectionTimeout > 0)
            timerSwiftlet.addInstantTimerListener(zombiConnectionTimeout, this);
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created");
    }

    public boolean isValid() {
        return !closed;
    }

    public String getDispatchToken() {
        return NetworkSwiftletImpl.TP_CONNHANDLER;
    }

    public String getDescription() {
        return connection.toString();
    }

    public void stop() {
        connectionManager.removeConnection(connection);
        closed = true;
    }

    public void internalClose() {
        closed = true;
    }

    public void performTimeAction() {
        if (traceSpace.enabled)
            traceSpace.trace("sys$net", toString() + "/perform time action: checking for zombi connections...");
        if (zombi) {
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/zombi connection detected, close!");
            logSwiftlet.logWarning("sys$net", toString() + "/zombi connection detected, close! Please check for possible denial-of-service attack!");
            stop();
        }
    }

    public void run() {
        InputStream in = connection.getInputStream();
        try {
            while (!closed) {
                byte[] buffer = inputHandler.getBuffer();
                int offset = inputHandler.getOffset();
                int n = in.read(buffer, offset, buffer.length - offset);
                if (n > 0) {
                    if (traceSpace.enabled)
                        traceSpace.trace("sys$net", toString() + "/inputHandler.setBytesWritten. n=" + n + ", closed=" + closed);
                    if (inputActiveIndicator != null)
                        inputActiveIndicator.set(true);
                    inputHandler.setBytesWritten(n);
                }
                if (n == -1)
                    throw new IOException("End-of-Stream reached");
                if (connection.isClosed() || connection.isMarkedForClose())
                    closed = true;
            }
        } catch (Exception e) {
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/Exception, EXITING: " + e);
            logSwiftlet.logInformation(toString(), "Exception, EXITING: " + e);
            if (!closed) {
                connectionManager.removeConnection(connection);
                closed = true;
            }
        }
    }

    public void chunkCompleted(byte[] b, int offset, int len) {
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/chunk completed");
        if (bais == null) {
            zombi = false;
            bais = new DataByteArrayInputStream();
        }
        bais.setBuffer(b, offset, len);
        try {
            inboundHandler.dataAvailable(connection, bais);
        } catch (Exception e) {
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/Exception, EXITING: " + e);
            logSwiftlet.logInformation(toString(), "Exception, EXITING: " + e);
            if (!closed) {
                connectionManager.removeConnection(connection);
                closed = true;
            }
        }
    }

    public String toString() {
        return connection.toString() + "/BlockingHandler";
    }
}

