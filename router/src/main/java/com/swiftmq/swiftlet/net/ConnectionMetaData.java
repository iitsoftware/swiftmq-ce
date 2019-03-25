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

package com.swiftmq.swiftlet.net;

import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import com.swiftmq.net.protocol.smqp.SMQPInputHandler;
import com.swiftmq.net.protocol.smqp.SMQPOutputHandler;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.net.event.ConnectionListener;

/**
 * A ConnectionMetaData object describes a connection and is a base class
 * for other meta data classes.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2004, All Rights Reserved
 */
public class ConnectionMetaData {
    Swiftlet swiftlet;
    long keepAliveInterval;
    String socketFactoryClass;
    ConnectionListener connectionListener;
    ProtocolInputHandler protocolInputHandler = null;
    ProtocolOutputHandler protocolOutputHandler = null;
    int inputBufferSize = 0;
    int inputExtendSize = 0;
    int outputBufferSize = 0;
    int outputExtendSize = 0;
    boolean useTcpNoDelay = true;
    int id;


    /**
     * Constructs a new ConnectionMetaData with default SMQP protocol handlers.
     *
     * @param swiftlet           Swiftlet.
     * @param keepAliveInterval  keepalive interval (for SMQP based connections only).
     * @param socketFactoryClass name of the socket factory class
     * @param connectionListener connection listener.
     * @param inputBufferSize    input network buffer size
     * @param inputExtendSize    input network extend size
     * @param outputBufferSize   output network buffer size
     * @param outputExtendSize   output network extend size
     * @param useTcpNoDelay      use TCP No Delay
     */
    protected ConnectionMetaData(Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                                 int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay) {
        this(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTcpNoDelay,
                new SMQPInputHandler(), new SMQPOutputHandler());
    }

    /**
     * Constructs a new ConnectionMetaData with custom protocol handlers.
     *
     * @param swiftlet              Swiftlet.
     * @param keepAliveInterval     keepalive interval (for SMQP based connections only).
     * @param socketFactoryClass    name of the socket factory class
     * @param connectionListener    connection listener.
     * @param inputBufferSize       input network buffer size
     * @param inputExtendSize       input network extend size
     * @param outputBufferSize      output network buffer size
     * @param outputExtendSize      output network extend size
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     * @param useTcpNoDelay         use TCP No Delay
     */
    protected ConnectionMetaData(Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                                 int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay,
                                 ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        this.swiftlet = swiftlet;
        this.keepAliveInterval = keepAliveInterval;
        this.socketFactoryClass = socketFactoryClass;
        this.connectionListener = connectionListener;
        this.inputBufferSize = inputBufferSize;
        this.inputExtendSize = inputExtendSize;
        this.outputBufferSize = outputBufferSize;
        this.outputExtendSize = outputExtendSize;
        this.protocolInputHandler = protocolInputHandler;
        this.protocolOutputHandler = protocolOutputHandler;
        this.useTcpNoDelay = useTcpNoDelay;
    }


    /**
     * Set a connection id.
     * Internal use only.
     *
     * @param id connection id.
     */
    public void setId(int id) {
        this.id = id;
    }


    /**
     * Returns the connection id.
     *
     * @return connection id.
     */
    public int getId() {
        return id;
    }


    /**
     * Returns the Swiftlet.
     *
     * @return Swiftlet.
     */
    public Swiftlet getSwiftlet() {
        // SBgen: Get variable
        return (swiftlet);
    }

    /**
     * Returns the keep alive interval.
     *
     * @return keep alive interval.
     */
    public long getKeepAliveInterval() {
        // SBgen: Get variable
        return (keepAliveInterval);
    }

    /**
     * Returns the name of the socket factory class.
     *
     * @return name of the socket factory class.
     */
    public String getSocketFactoryClass() {
        // SBgen: Get variable
        return (socketFactoryClass);
    }

    /**
     * Returns the connection listener.
     *
     * @return connection listener.
     */
    public ConnectionListener getConnectionListener() {
        // SBgen: Get variable
        return (connectionListener);
    }

    /**
     * Returns the input buffer size.
     *
     * @return input buffer size.
     */
    public int getInputBufferSize() {
        return inputBufferSize;
    }

    /**
     * Returns the input extend size.
     *
     * @return input extend size.
     */
    public int getInputExtendSize() {
        return inputExtendSize;
    }

    /**
     * Returns the output buffer size.
     *
     * @return output buffer size.
     */
    public int getOutputBufferSize() {
        return outputBufferSize;
    }

    /**
     * Returns the output extend size.
     *
     * @return output extend size.
     */
    public int getOutputExtendSize() {
        return outputExtendSize;
    }

    /**
     * Returns the TCP No Delay setting
     *
     * @return TCP No Delay setting.
     */
    public boolean isUseTcpNoDelay() {
        return useTcpNoDelay;
    }

    /**
     * Returns the protocol input handler.
     *
     * @return protocol input handler.
     */
    public ProtocolInputHandler createProtocolInputHandler() {
        // SBgen: Get variable
        return protocolInputHandler.create();
    }

    /**
     * Returns the protocol input handler.
     *
     * @return protocol input handler.
     */
    public ProtocolOutputHandler createProtocolOutputHandler() {
        // SBgen: Get variable
        return protocolOutputHandler.create(outputBufferSize, outputExtendSize);
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append("[ConnectionMetaData, swiftlet=");
        b.append(swiftlet.getName());
        b.append(", keepAliveInterval=");
        b.append(keepAliveInterval);
        b.append(", socketFactoryClass=");
        b.append(socketFactoryClass);
        b.append(", connectionListener=");
        b.append(connectionListener);
        b.append(", protocolInputHandler=");
        b.append(protocolInputHandler);
        b.append(", protocolOutputHandler=");
        b.append(protocolOutputHandler);
        b.append(", inputBufferSize=");
        b.append(inputBufferSize);
        b.append(", inputExtendSize=");
        b.append(inputExtendSize);
        b.append(", outputBufferSize=");
        b.append(outputBufferSize);
        b.append(", outputExtendSize=");
        b.append(outputExtendSize);
        b.append(", useTcpNoDelay=");
        b.append(useTcpNoDelay);
        b.append("]");
        return b.toString();
    }
}

