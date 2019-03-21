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
import com.swiftmq.net.protocol.OutputListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import com.swiftmq.net.protocol.smqp.SMQPInputHandler;
import com.swiftmq.net.protocol.smqp.SMQPOutputHandler;
import com.swiftmq.tools.prop.SystemProperties;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingConnection extends Thread
        implements Connection, ChunkListener, OutputListener {
    static final boolean ISDAEMON = Boolean.valueOf(SystemProperties.get("swiftmq.socket.reader.isdaemon", "false")).booleanValue();
    static final boolean SET_SOCKET_OPTIONS = Boolean.valueOf(SystemProperties.get("swiftmq.socket.set.options", "true")).booleanValue();
    static final int MAX_SNDBUFSIZE = Integer.parseInt(SystemProperties.get("swiftmq.socket.max.sendbuffersize", "0"));
    static final int MAX_RCVBUFSIZE = Integer.parseInt(SystemProperties.get("swiftmq.socket.max.receivebuffersize", "0"));
    static final int SO_TIMEOUT = Integer.parseInt(SystemProperties.get("swiftmq.socket.sotimeout", "0"));
    Socket socket = null;
    int inputBufferSize = 0;
    int inputExtendSize = 0;
    int outputBufferSize = 0;
    int outputExtendSize = 0;
    ProtocolInputHandler inputHandler = null;
    ProtocolOutputHandler outputHandler = null;
    DataByteArrayInputStream dis = null;
    InboundHandler inboundHandler = null;
    ExceptionHandler exceptionHandler = null;
    InputStream socketIn = null;
    OutputStream socketOut = null;
    String myHostname = null;
    boolean closed = false;
    int sndBufferSize = 8192;
    AtomicBoolean inputActiveIndicator = null;

    public BlockingConnection(Socket socket, InboundHandler inboundHandler, ExceptionHandler exceptionHandler) throws IOException {
        this(socket, 128 * 1024, 64 * 1024, 128 * 1024, 64 * 1024);
        this.inboundHandler = inboundHandler;
        this.exceptionHandler = exceptionHandler;
    }

    public BlockingConnection(Socket socket, int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize) throws IOException {
        this.socket = socket;
        setDaemon(ISDAEMON);
        if (SET_SOCKET_OPTIONS) {
            int n = outputBufferSize;
            try {
                if (MAX_SNDBUFSIZE > 0)
                    n = Math.min(outputBufferSize, MAX_SNDBUFSIZE);
                socket.setSendBufferSize(n);
                sndBufferSize = socket.getSendBufferSize();
            } catch (SocketException e) {
                System.err.println("Unable to perform 'socket.setSendBufferSize(" + n + ")', exception: " + e);
            }
            try {
                n = inputBufferSize;
                if (MAX_RCVBUFSIZE > 0)
                    n = Math.min(inputBufferSize, MAX_RCVBUFSIZE);
                if (socket.getReceiveBufferSize() != n)
                    socket.setReceiveBufferSize(n);
            } catch (SocketException e) {
                System.err.println("Unable to perform 'socket.setReceiveBufferSize(" + n + ")', exception: " + e);
            }
        }
        if (SO_TIMEOUT > 0) {
            try {
                socket.setSoTimeout(SO_TIMEOUT);
            } catch (SocketException e) {
                System.err.println("Unable to perform 'socket.setSoTimeout(" + SO_TIMEOUT + ")', exception: " + e);
            }
        }
        this.inputBufferSize = inputBufferSize;
        this.inputExtendSize = inputExtendSize;
        this.outputBufferSize = outputBufferSize;
        this.outputExtendSize = outputExtendSize;
        outputHandler = createOutputHandler(outputBufferSize, outputExtendSize);
        outputHandler.setOutputListener(this);
        inputHandler = createInputHandler();
        inputHandler.createInputBuffer(inputBufferSize, inputExtendSize);
        inputHandler.setChunkListener(this);
        dis = new DataByteArrayInputStream();
        socketIn = socket.getInputStream();
        socketOut = socket.getOutputStream();
        try {
            myHostname = socket.getLocalAddress().toString();
        } catch (Exception e) {
            myHostname = "unknown";
        }
    }

    protected ProtocolOutputHandler createOutputHandler(int outputBufferSize, int outputExtendSize) {
        return new SMQPOutputHandler(outputBufferSize, outputExtendSize) {
            public void flush() throws IOException {
                super.flush();
                invokeOutputListener();
            }
        };
    }

    protected ProtocolInputHandler createInputHandler() {
        return new SMQPInputHandler();
    }

    public void setInputActiveIndicator(AtomicBoolean inputActiveIndicator) {
        this.inputActiveIndicator = inputActiveIndicator;
    }

    public synchronized void chunkCompleted(byte[] b, int offset, int len) {
        dis.setBuffer(b, offset, len);
        inboundHandler.dataAvailable(dis);
    }

    public int performWrite(byte[] b, int offset, int len)
            throws IOException {
        socketOut.write(b, offset, len);
        socketOut.flush();
        return len;
    }

    public void run() {
        try {
            while (!closed) {
                byte[] buffer = inputHandler.getBuffer();
                int offset = inputHandler.getOffset();
                try {
                    int n = socketIn.read(buffer, offset, buffer.length - offset);
                    if (n > 0) {
                        if (inputActiveIndicator != null)
                            inputActiveIndicator.set(true);
                        inputHandler.setBytesWritten(n);
                    }
                    if (n == -1)
                        throw new IOException("End-of-Stream reached");
                } catch (SocketTimeoutException e) {
                }
            }
        } catch (IOException e) {
            if (!closed && exceptionHandler != null)
                exceptionHandler.onException(e);
        }
    }

    public synchronized void setInboundHandler(InboundHandler inboundHandler) {
        this.inboundHandler = inboundHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public String getLocalHostname() {
        return myHostname;
    }

    public String getHostname() {
        return socket.getInetAddress().getHostName();
    }

    public int getPort() {
        return socket.getPort();
    }

    public OutputStream getOutputStream() {
        return outputHandler;
    }

    public void close() {
        closed = true;
        try {
            socket.close();
        } catch (IOException e) {
        }
    }

    public String toString() {
        return "[BlockingConnection, socket=" + socket + "]";
    }
}
