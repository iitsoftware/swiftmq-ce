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
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.net.event.ConnectionListener;

/**
 * A ConnectorMetaData object describes a TCP connector.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class ConnectorMetaData extends ConnectionMetaData {
    String hostname;
    int port;
    long retryInterval;


    /**
     * Constructs a new ConnectorMetaData.
     *
     * @param hostname           hostname to connect to.
     * @param port               port to connect to.
     * @param retryInterval      retry interval if a connect attempt fails.
     * @param swiftlet           Swiftlet.
     * @param keepAliveInterval  keep alive interval (for SMQP based connections only).
     * @param socketFactoryClass name of the socket factory class.
     * @param connectionListener connection listener.
     * @param inputBufferSize    input network buffer size
     * @param inputExtendSize    input network extend size
     * @param outputBufferSize   output network buffer size
     * @param outputExtendSize   output network extend size
     */
    public ConnectorMetaData(String hostname, int port, long retryInterval, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                             int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, true);
        this.hostname = hostname;
        this.port = port;
        this.retryInterval = retryInterval;
    }

    /**
     * Constructs a new ConnectorMetaData.
     *
     * @param hostname           hostname to connect to.
     * @param port               port to connect to.
     * @param retryInterval      retry interval if a connect attempt fails.
     * @param swiftlet           Swiftlet.
     * @param keepAliveInterval  keep alive interval (for SMQP based connections only).
     * @param socketFactoryClass name of the socket factory class.
     * @param connectionListener connection listener.
     * @param inputBufferSize    input network buffer size
     * @param inputExtendSize    input network extend size
     * @param outputBufferSize   output network buffer size
     * @param outputExtendSize   output network extend size
     * @param useTcpNoDelay      use TCP No Delay
     */
    public ConnectorMetaData(String hostname, int port, long retryInterval, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                             int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTcpNoDelay);
        this.hostname = hostname;
        this.port = port;
        this.retryInterval = retryInterval;
    }

    /**
     * Constructs a new ConnectorMetaData.
     *
     * @param hostname              hostname to connect to.
     * @param port                  port to connect to.
     * @param retryInterval         retry interval if a connect attempt fails.
     * @param swiftlet              Swiftlet.
     * @param keepAliveInterval     keep alive interval (for SMQP based connections only).
     * @param socketFactoryClass    name of the socket factory class.
     * @param connectionListener    connection listener.
     * @param inputBufferSize       input network buffer size
     * @param inputExtendSize       input network extend size
     * @param outputBufferSize      output network buffer size
     * @param outputExtendSize      output network extend size
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     */
    public ConnectorMetaData(String hostname, int port, long retryInterval, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                             int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize,
                             ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, true, protocolInputHandler, protocolOutputHandler);
        this.hostname = hostname;
        this.port = port;
        this.retryInterval = retryInterval;
    }

    /**
     * Constructs a new ConnectorMetaData.
     *
     * @param hostname              hostname to connect to.
     * @param port                  port to connect to.
     * @param retryInterval         retry interval if a connect attempt fails.
     * @param swiftlet              Swiftlet.
     * @param keepAliveInterval     keep alive interval (for SMQP based connections only).
     * @param socketFactoryClass    name of the socket factory class.
     * @param connectionListener    connection listener.
     * @param inputBufferSize       input network buffer size
     * @param inputExtendSize       input network extend size
     * @param outputBufferSize      output network buffer size
     * @param outputExtendSize      output network extend size
     * @param useTcpNoDelay         use TCP No Delay
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     */
    public ConnectorMetaData(String hostname, int port, long retryInterval, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                             int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay,
                             ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTcpNoDelay, protocolInputHandler, protocolOutputHandler);
        this.hostname = hostname;
        this.port = port;
        this.retryInterval = retryInterval;
    }


    /**
     * Returns the hostname.
     *
     * @return hostname.
     */
    public String getHostname() {
        // SBgen: Get variable
        return (hostname);
    }

    /**
     * Returns the port.
     *
     * @return port.
     */
    public int getPort() {
        // SBgen: Get variable
        return (port);
    }

    /**
     * Returns the retry interval.
     *
     * @return retry interval.
     */
    public long getRetryInterval() {
        // SBgen: Get variable
        return (retryInterval);
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append("[ConnectorMetaData, ");
        b.append(super.toString());
        b.append(", hostname=");
        b.append(hostname);
        b.append(", port=");
        b.append(port);
        b.append(", retryInterval=");
        b.append(retryInterval);
        b.append("]");
        return b.toString();
    }
}

