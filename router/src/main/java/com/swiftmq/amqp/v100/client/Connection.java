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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.OutboundHandler;
import com.swiftmq.amqp.Writable;
import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.amqp.v100.client.po.POAuthenticate;
import com.swiftmq.amqp.v100.client.po.POOpen;
import com.swiftmq.amqp.v100.client.po.POProtocolRequest;
import com.swiftmq.amqp.v100.client.po.POSendClose;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import com.swiftmq.amqp.v100.types.Util;
import com.swiftmq.net.PlainSocketFactory;
import com.swiftmq.net.SocketFactory;
import com.swiftmq.net.SocketFactory2;
import com.swiftmq.net.client.BlockingConnection;
import com.swiftmq.net.client.ExceptionHandler;
import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import com.swiftmq.net.protocol.raw.RawOutputHandler;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.util.DataStreamOutputStream;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Representation of an AMQP connection.
 * <p/>
 * The actual connect is done by calling the "connect()" method. Any attribute change, e.g. buffer size, must be done before
 * this method is called.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class Connection implements ExceptionHandler {
    AMQPContext ctx = null;
    Tracer fTracer = null;
    String hostname = null;
    String openHostname = System.getProperty("swiftmq.amqp.open.hostname");
    int port;
    String mechanism = "PLAIN";
    String userName = null;
    String password = null;
    long idleTimeout = Integer.MAX_VALUE;
    long maxFrameSize = Integer.MAX_VALUE;
    com.swiftmq.net.client.Connection networkConnection = null;
    ConnectionDispatcher connectionDispatcher = null;
    ThreadPool connectionPool = null;
    ConnectionQueue connectionQueue = null;
    ConnectionTask connectionTask = null;
    DataStreamOutputStream dos = null;
    volatile boolean closed = false;
    ArrayList localChannels = new ArrayList();
    ArrayList remoteChannels = new ArrayList();
    Lock lock = new ReentrantLock();
    boolean doAuth = true;
    String containerId = null;
    boolean containerIdSet = false;
    boolean connected = false;
    int inputBufferSize = 131072;
    int inputBufferExtendSize = 65536;
    int outputBufferSize = 131072;
    int outputBufferExtendSize = 65536;
    SocketFactory socketFactory = new PlainSocketFactory();
    ExceptionListener exceptionListener = null;

    /**
     * Creates a Connection and uses SASL for authentication.
     *
     * @param ctx      AMQP context
     * @param hostname Hostname.
     * @param port     Port
     * @param userName Username
     * @param password Password
     */
    public Connection(AMQPContext ctx, String hostname, int port, String userName, String password) {
        this.ctx = ctx;
        this.hostname = hostname;
        this.port = port;
        this.userName = userName;
        this.password = password;
        String myHostname = "unknown";
        try {
            myHostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
        }
        containerId = UUID.randomUUID().toString() + "@" + myHostname;
        fTracer = ctx.getFrameTracer();
    }

    /**
     * Creates a Connection and either does an anonymous login via SASL (doAuth is true) or
     * avoids SASL and starts directly with the AMQP protocol (doAuth is false).
     *
     * @param ctx      AMQP context
     * @param hostname Hostname
     * @param port     Port
     * @param doAuth   do authentication as anonymous via SASL or do not use SASL at all
     */
    public Connection(AMQPContext ctx, String hostname, int port, boolean doAuth) {
        this(ctx, hostname, port, null, null);
        this.doAuth = doAuth;
    }

    private void verifyState() throws ConnectionClosedException {
        if (closed)
            throw new ConnectionClosedException("Connection is closed");
    }

    /**
     * Returns the exception listener
     *
     * @return exception listener
     */
    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    /**
     * Sets the exception listener.
     *
     * @param exceptionListener exception listener
     */
    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    /**
     * Returns the maximum frame size
     *
     * @return max frame size
     */
    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the maximum frame size. Default is Integer.MAX_VALUE.
     *
     * @param maxFrameSize max frame size
     */
    public void setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    /**
     * Sets the SASL mechanisms to use for authentication. If SwiftMQ is the remote server,
     * it supports PLAIN, CRAM-MD5, Digest-MD5. For ANONYMOUS (which SwiftMQ supports as well)
     * please use the resp. constructor of this connection.
     *
     * @param mechanism the SASL mechanisms to use
     */
    public void setMechanism(String mechanism) {
        this.mechanism = mechanism;
    }

    /**
     * Returns the container id
     *
     * @return container id
     */
    public String getContainerId() {
        return containerId;
    }

    /**
     * Sets the container id. A container id is automatically and randomly generated if it is not set.
     * For SwiftMQ the container id is used to identify durable subscribers. It is the same
     * as the client id in JMS.
     *
     * @param containerId container id
     */
    public void setContainerId(String containerId) {
        this.containerId = containerId;
        containerIdSet = true;
    }

    /**
     * Returns the hostname set in the open frame.
     *
     * @return hostname
     */
    public String getOpenHostname() {
        return openHostname;
    }

    /**
     * Sets (overwrites) the hostname that will be set in the open frame. Some AMQP brokers
     * may use this field to select a backend service and use this field as a "virtual host".
     *
     * @param openHostname open hostname
     */
    public void setOpenHostname(String openHostname) {
        this.openHostname = openHostname;
    }

    /**
     * Sets the idle timeout. Default is Long.MAX_VALUE.
     * <p/>
     * If a connection does not send/receive data within this interval, the connection is closed. The SwiftMQ client
     * sends heart beat messages every idletimeout/2 millisecond.
     *
     * @param idleTimeout idle timeout in milliseconds
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Returns the input buffer size
     *
     * @return input buffer size
     */
    public int getInputBufferSize() {
        return inputBufferSize;
    }

    /**
     * Sets the input buffer size. This is the size of the network buffer SwiftMQ uses to receive frames. Default is 131072 bytes.
     *
     * @param inputBufferSize input buffer size
     */
    public void setInputBufferSize(int inputBufferSize) {
        this.inputBufferSize = inputBufferSize;
    }

    /**
     * Returns the input buffer extend size
     *
     * @return input buffer extend size
     */
    public int getInputBufferExtendSize() {
        return inputBufferExtendSize;
    }

    /**
     * Sets the input buffer extend size. This is the size on which the input buffer is extended if its size isn't sufficient to receive a frame.
     * Default is 65536 bytes.
     *
     * @param inputBufferExtendSize input buffer extend size
     */
    public void setInputBufferExtendSize(int inputBufferExtendSize) {
        this.inputBufferExtendSize = inputBufferExtendSize;
    }

    /**
     * Returns the output buffer size
     *
     * @return output buffer size
     */
    public int getOutputBufferSize() {
        return outputBufferSize;
    }

    /**
     * Sets the output buffer size. This is the size of the network buffer SwiftMQ uses to send frames. Default is 131072 bytes.
     *
     * @param outputBufferSize output buffer size
     */
    public void setOutputBufferSize(int outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    /**
     * Returns the output buffer extend size
     *
     * @return output buffer extend size
     */
    public int getOutputBufferExtendSize() {
        return outputBufferExtendSize;
    }

    /**
     * Sets the output buffer extend size. This is the size on which the output buffer is extended if its size isn't sufficient to send a frame.
     * Default is 65536 bytes.
     *
     * @param outputBufferExtendSize output buffer extend size
     */
    public void setOutputBufferExtendSize(int outputBufferExtendSize) {
        this.outputBufferExtendSize = outputBufferExtendSize;
    }

    /**
     * Returns the exception listener
     *
     * @return exception listener
     */
    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     * Sets the socket factory. Default is com.swiftmq.net.PlainSocketFactory. To use SSL/TLS, specify
     * com.swiftmq.net.JSSESocketFactory.
     *
     * @param socketFactory socket factory
     */
    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /**
     * Returns the user name
     *
     * @return user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Performs the actual connect to the remote host, negotiates the protocol and authenticates the user
     *
     * @throws IOException                         if an IOExcption occurs
     * @throws UnsupportedProtocolVersionException if the AMQP/SASL protocol version is not supported by the remote host
     * @throws AuthenticationException             if the user cannot be authenticated
     * @throws ConnectionClosedException           if the connection was closed
     */
    public void connect()
            throws IOException, UnsupportedProtocolVersionException, AuthenticationException, ConnectionClosedException {
        verifyState();
        connectionDispatcher = new ConnectionDispatcher(ctx, hostname);
        if (socketFactory instanceof SocketFactory2)
            ((SocketFactory2) socketFactory).setReceiveBufferSize(inputBufferSize);
        networkConnection = new BlockingConnection(socketFactory.createSocket(hostname, port), connectionDispatcher, this) {
            protected ProtocolOutputHandler createOutputHandler(int outputBufferSize, int outputExtendSize) {
                return new RawOutputHandler(outputBufferSize, outputExtendSize) {
                    public void flush() throws IOException {
                        super.flush();
                        invokeOutputListener();
                    }
                };
            }

            protected ProtocolInputHandler createInputHandler() {
                return connectionDispatcher.getProtocolHandler();
            }
        };
        connectionDispatcher.setMyConnection(this);
        networkConnection.start();
        dos = new DataStreamOutputStream(networkConnection.getOutputStream());
        connectionPool = ctx.getConnectionPool();
        connectionTask = new ConnectionTask();
        connectionQueue = new ConnectionQueue();
        connectionDispatcher.setOutboundHandler(connectionQueue);
        connectionQueue.startQueue();

        // SASL
        if (doAuth) {
            connectionDispatcher.setSaslActive(true);
            Semaphore sem = new Semaphore();
            POObject po = new POProtocolRequest(sem, Util.SASL_INIT);
            connectionDispatcher.dispatch(po);
            sem.waitHere();
            sem.reset();
            if (!po.isSuccess()) {
                cancel();
                throw new UnsupportedProtocolVersionException(po.getException());
            }
            po = new POAuthenticate(sem, mechanism, userName, password);
            connectionDispatcher.dispatch(po);
            sem.waitHere();
            sem.reset();
            if (!po.isSuccess()) {
                cancel();
                throw new AuthenticationException(po.getException());
            }
        }

        // AMQP
        Semaphore sem = new Semaphore();
        POObject po = new POProtocolRequest(sem, Util.AMQP_INIT);
        connectionDispatcher.dispatch(po);
        sem.waitHere();
        sem.reset();
        if (!po.isSuccess()) {
            cancel();
            throw new UnsupportedProtocolVersionException(po.getException());
        }

        // Open
        po = new POOpen(sem, containerId, maxFrameSize, 255, idleTimeout);
        connectionDispatcher.dispatch(po);
        sem.waitHere();
        sem.reset();
        if (!po.isSuccess()) {
            cancel();
            throw new IOException(po.getException());
        }
        connected = true;
    }

    /**
     * Internal use.
     *
     * @param e IOException
     */
    public void onException(final IOException e) {
        new Thread() {
            public void run() {
                cancel();
                if (exceptionListener != null)
                    exceptionListener.onException(new ConnectionClosedException(e.toString()));
            }
        }.start();
    }

    private Session mapSessionToLocalChannel(long incomingWindowSize, long outgoingWindowSize) {
        lock.lock();
        try {
            Session session = new Session(ctx, this, incomingWindowSize, outgoingWindowSize);
            session.setChannel(ArrayListTool.setFirstFreeOrExpand(localChannels, session));
            return session;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a new Session.
     *
     * @param incomingWindowSize Incoming Window Size (maxnumber of unsettled incoming transfers)
     * @param outgoingWindowSize Outgoing Window Size (max number of unsettled outgoing transfers)
     * @return Session
     * @throws SessionHandshakeException An error occured during handshake
     * @throws ConnectionClosedException The connection was closed
     */
    public Session createSession(long incomingWindowSize, long outgoingWindowSize) throws SessionHandshakeException, ConnectionClosedException {
        verifyState();
        if (!connected)
            throw new SessionHandshakeException("Connection is not connected, call 'connect()'");
        Session session = mapSessionToLocalChannel(incomingWindowSize, outgoingWindowSize);
        session.finishHandshake();
        return session;
    }

    protected void removeSession(Session session) {
        lock.lock();
        try {
            localChannels.set(session.getChannel(), null);
        } finally {
            lock.unlock();
        }
    }

    protected Session getSessionForLocalChannel(int localChannel) {
        lock.lock();
        try {
            if (localChannel >= 0 && localChannel < localChannels.size())
                return (Session) localChannels.get(localChannel);
            return null;
        } finally {
            lock.unlock();
        }
    }

    protected void mapSessionToRemoteChannel(Session session, int remoteChannel) {
        lock.lock();
        try {
            Util.ensureSize(remoteChannels, remoteChannel + 1);
            remoteChannels.set(remoteChannel, session);
        } finally {
            lock.unlock();
        }
    }

    protected void unmapSessionFromRemoteChannel(int remoteChannel) {
        lock.lock();
        try {
            Util.ensureSize(remoteChannels, remoteChannel + 1);
            remoteChannels.set(remoteChannel, null);
        } finally {
            lock.unlock();
        }
    }

    protected Session getSessionForRemoteChannel(int remoteChannel) {
        lock.lock();
        try {
            if (remoteChannel >= 0 && remoteChannel < remoteChannels.size())
                return (Session) remoteChannels.get(remoteChannel);
            return null;
        } finally {
            lock.unlock();
        }
    }

    protected OutboundHandler getOutboundHandler() {
        return connectionQueue;
    }

    /**
     * Internal use only
     */
    public void cancel() {
        if (closed)
            return;
        List cloned = null;
        lock.lock();
        try {
            if (closed)
                return;
            cloned = (List) ((ArrayList) localChannels).clone();
        } finally {
            lock.unlock();
        }
        for (int i = 0; i < cloned.size(); i++) {
            Session session = (Session) cloned.get(i);
            if (session != null)
                session.cancel();
        }
        if (connectionDispatcher != null)
            connectionDispatcher.close();
        if (connectionQueue != null)
            connectionQueue.stopQueue();
        if (networkConnection != null)
            networkConnection.close();
        closed = true;
    }

    /**
     * Close this connection and all sessions created from it.
     */
    public void close() {
        close(null, null);
    }

    protected void close(String condition, String description) {
        if (closed)
            return;
        if (condition == null) {
            Semaphore sem = new Semaphore();
            POSendClose po = new POSendClose(sem, condition == null ? null : new AMQPSymbol(condition), description == null ? null : new AMQPString(description));
            connectionDispatcher.dispatch(po);
            sem.waitHere();
        } else {
            POSendClose po = new POSendClose(null, condition == null ? null : new AMQPSymbol(condition), description == null ? null : new AMQPString(description));
            connectionDispatcher.dispatch(po);
        }
        cancel();
        if (exceptionListener != null && condition != null && description != null)
            exceptionListener.onException(new ConnectionClosedException(condition + " / " + description));
    }

    private class ConnectionQueue extends SingleProcessorQueue implements OutboundHandler {
        public ConnectionQueue() {
            super(100);
        }

        protected void startProcessor() {
            if (!closed)
                connectionPool.dispatchTask(connectionTask);
        }

        public void send(Writable writable) {
            enqueue(writable);
        }

        protected void process(Object[] bulk, int n) {
            try {
                for (int i = 0; i < n; i++) {
                    ((Writable) bulk[i]).writeContent(dos);
                    if (fTracer.isEnabled()) {
                        if (bulk[i] instanceof AMQPFrame)
                            fTracer.trace("amqp", "SND[" + ((AMQPFrame) bulk[i]).getChannel() + "] (size=" + ((AMQPFrame) bulk[i]).getPredictedSize() + "): " + bulk[i]);
                        else
                            fTracer.trace("amqp", "SND: " + bulk[i]);
                    }
                }
                dos.flush();
            } catch (Exception e) {
                cancel();
            } finally {
                for (int i = 0; i < n; i++) {
                    Writable w = (Writable) bulk[i];
                    if (w.getSemaphore() != null)
                        w.getSemaphore().notifySingleWaiter();
                    else if (w.getCallback() != null)
                        w.getCallback().done(true);
                }
            }
        }
    }

    private class ConnectionTask implements AsyncTask {
        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return "connectiontask";
        }

        public String getDescription() {
            return "Connection/ConnectionTask";
        }

        public void run() {
            if (!closed && connectionQueue.dequeue())
                connectionPool.dispatchTask(this);
        }

        public void stop() {
        }
    }
}