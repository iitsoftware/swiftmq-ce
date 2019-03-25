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
import com.swiftmq.net.SocketFactory;
import com.swiftmq.net.SocketFactory2;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.net.ConnectionVetoException;
import com.swiftmq.swiftlet.net.ConnectorMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.net.Socket;
import java.net.SocketException;

public class BlockingTCPConnector extends TCPConnector {
    SocketFactory socketFactory = null;
    NetworkSwiftletImpl networkSwiftlet = null;
    ThreadpoolSwiftlet threadpoolSwiftlet = null;
    LogSwiftlet logSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    ConnectionManager connectionManager = null;
    BlockingHandler blockingHandler = null;

    public BlockingTCPConnector(ConnectorMetaData metaData, SocketFactory socketFactory) {
        super(metaData);
        this.socketFactory = socketFactory;
        networkSwiftlet = (NetworkSwiftletImpl) SwiftletManager.getInstance().getSwiftlet("sys$net");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        connectionManager = networkSwiftlet.getConnectionManager();
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        if (networkSwiftlet.isSetSocketOptions() && socketFactory instanceof SocketFactory2)
            ((SocketFactory2) socketFactory).setReceiveBufferSize(metaData.getInputBufferSize());
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created");
    }

    public synchronized void connect() {
        if (blockingHandler != null && blockingHandler.isValid())
            return;
        if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/try to connect ...");
        try {
            Socket socket = socketFactory.createSocket(getMetaData().getHostname(), getMetaData().getPort(), getMetaData().isUseTcpNoDelay());
            if (networkSwiftlet.isSetSocketOptions()) {
                try {
                    socket.setSendBufferSize(getMetaData().getOutputBufferSize());
                } catch (SocketException e) {
                    if (traceSpace.enabled)
                        traceSpace.trace("sys$net", toString() + "/unable to perform 'socket.setSendBufferSize(" + getMetaData().getOutputBufferSize() + ")', exception: " + e);
                    logSwiftlet.logWarning(toString(), "/unable to perform 'socket.setSendBufferSize(" + getMetaData().getOutputBufferSize() + ")', exception: " + e);
                }
                if (socket.getReceiveBufferSize() != getMetaData().getInputBufferSize()) {
                    try {
                        socket.setReceiveBufferSize(getMetaData().getInputBufferSize());
                    } catch (SocketException e) {
                        if (traceSpace.enabled)
                            traceSpace.trace("sys$net", toString() + "/unable to perform 'socket.setReceiveBufferSize(" + getMetaData().getInputBufferSize() + ")', exception: " + e);
                        logSwiftlet.logWarning(toString(), "/unable to perform 'socket.setReceiveBufferSize(" + getMetaData().getInputBufferSize() + ")', exception: " + e);
                    }
                }
            }
            logSwiftlet.logInformation(toString(), "connected to: " + socket.getInetAddress().getHostAddress());
            TCPConnection connection = new TCPConnection(networkSwiftlet.isDnsResolve(), getMetaData().isUseTcpNoDelay(), socket);
            try {
                ConnectionListener connectionListener = getMetaData().getConnectionListener();
                connection.setConnectionListener(connectionListener);
                connection.setMetaData(getMetaData());
                connectionListener.connected(connection);
                connectionManager.addConnection(connection);
                blockingHandler = new BlockingHandler(connection);
                threadpoolSwiftlet.dispatchTask(blockingHandler);
            } catch (ConnectionVetoException cve) {
                connection.close();
                if (traceSpace.enabled)
                    traceSpace.trace("sys$net", toString() + "/ConnectionVetoException: " + cve.getMessage());
                logSwiftlet.logError(toString(), "ConnectionVetoException: " + cve.getMessage());
            }
        } catch (Exception e) {
            blockingHandler = null;
            if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/unable to connect, exception: " + e);
        }
    }

    public void close() {
        super.close();
        if (blockingHandler != null)
            blockingHandler.stop();
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append("[BlockingTCPConnector, ");
        b.append(super.toString());
        b.append(']');
        return b.toString();
    }
}

