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
import com.swiftmq.tools.sql.LikeComparator;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A ListenerMetaData object describes a TCP listener.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class ListenerMetaData extends ConnectionMetaData {
    InetAddress bindAddress;
    int port;
    HashSet hostAccessList = null;


    /**
     * Constructs a new ListenerMetaData.
     *
     * @param bindAddress        IP bind address
     * @param port               port.
     * @param swiftlet           Swiftlet.
     * @param keepAliveInterval  keep alive interval.
     * @param socketFactoryClass name of the socket factory class.
     * @param connectionListener connection listener.
     * @param inputBufferSize    input network buffer size
     * @param inputExtendSize    input network extend size
     * @param outputBufferSize   output network buffer size
     * @param outputExtendSize   output network extend size
     * @see package.class
     */
    public ListenerMetaData(InetAddress bindAddress, int port, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                            int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, true);
        this.bindAddress = bindAddress;
        this.port = port;
    }

    /**
     * Constructs a new ListenerMetaData.
     *
     * @param bindAddress        IP bind address
     * @param port               port.
     * @param swiftlet           Swiftlet.
     * @param keepAliveInterval  keep alive interval.
     * @param socketFactoryClass name of the socket factory class.
     * @param connectionListener connection listener.
     * @param inputBufferSize    input network buffer size
     * @param inputExtendSize    input network extend size
     * @param outputBufferSize   output network buffer size
     * @param outputExtendSize   output network extend size
     * @param useTcpNoDelay      use TCP No Delay
     * @see package.class
     */
    public ListenerMetaData(InetAddress bindAddress, int port, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                            int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTcpNoDelay);
        this.bindAddress = bindAddress;
        this.port = port;
    }

    /**
     * Constructs a new ListenerMetaData.
     *
     * @param bindAddress           IP bind address
     * @param port                  port.
     * @param swiftlet              Swiftlet.
     * @param keepAliveInterval     keep alive interval.
     * @param socketFactoryClass    name of the socket factory class.
     * @param connectionListener    connection listener.
     * @param inputBufferSize       input network buffer size
     * @param inputExtendSize       input network extend size
     * @param outputBufferSize      output network buffer size
     * @param outputExtendSize      output network extend size
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     * @see package.class
     */
    public ListenerMetaData(InetAddress bindAddress, int port, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                            int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize,
                            ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, true, protocolInputHandler, protocolOutputHandler);
        this.bindAddress = bindAddress;
        this.port = port;
    }

    /**
     * Constructs a new ListenerMetaData.
     *
     * @param bindAddress           IP bind address
     * @param port                  port.
     * @param swiftlet              Swiftlet.
     * @param keepAliveInterval     keep alive interval.
     * @param socketFactoryClass    name of the socket factory class.
     * @param connectionListener    connection listener.
     * @param inputBufferSize       input network buffer size
     * @param inputExtendSize       input network extend size
     * @param outputBufferSize      output network buffer size
     * @param outputExtendSize      output network extend size
     * @param useTcpNoDelay         use TCP No Delay
     * @param protocolInputHandler  protocol input handler.
     * @param protocolOutputHandler protocol output handler.
     * @see package.class
     */
    public ListenerMetaData(InetAddress bindAddress, int port, Swiftlet swiftlet, long keepAliveInterval, String socketFactoryClass, ConnectionListener connectionListener,
                            int inputBufferSize, int inputExtendSize, int outputBufferSize, int outputExtendSize, boolean useTcpNoDelay,
                            ProtocolInputHandler protocolInputHandler, ProtocolOutputHandler protocolOutputHandler) {
        super(swiftlet, keepAliveInterval, socketFactoryClass, connectionListener,
                inputBufferSize, inputExtendSize, outputBufferSize, outputExtendSize, useTcpNoDelay, protocolInputHandler, protocolOutputHandler);
        this.bindAddress = bindAddress;
        this.port = port;
    }


    /**
     * Returns the bind address.
     *
     * @return bind address.
     */
    public InetAddress getBindAddress() {
        // SBgen: Get variable
        return (bindAddress);
    }

    /**
     * Returns the port.
     *
     * @return bind port.
     */
    public int getPort() {
        // SBgen: Get variable
        return (port);
    }


    /**
     * Adds a new predicate to the host access list.
     * The predicate is a SQL Like predicate which is compared against the
     * remote host name on connect. Only those hosts which matches at least
     * one predicate are allowed to connect, otherwise the connection will
     * be rejected.
     *
     * @param predicate SQL Like predicate.
     */
    public synchronized void addToHostAccessList(String predicate) {
        if (hostAccessList == null)
            hostAccessList = new HashSet();
        hostAccessList.add(predicate);
    }


    /**
     * Removes a predicate from the host access list.
     *
     * @param predicate SQL Like predicate.
     */
    public synchronized void removeFromHostAccessList(String predicate) {
        if (hostAccessList != null)
            hostAccessList.remove(predicate);
    }


    /**
     * Returns if the given hostname is allowed to connect.
     * This method compares the hostname against the host access list.
     * Internal use only.
     *
     * @param hostname name of the remote host.
     * @return true/false.
     */
    public boolean isConnectionAllowed(String hostname) {
        if (hostAccessList == null)
            return true;
        else {
            boolean rc = false;
            Iterator iter = hostAccessList.iterator();
            while (iter.hasNext()) {
                if (LikeComparator.compare(hostname, (String) iter.next(), '\\')) {
                    rc = true;
                    break;
                }
            }
            return rc;
        }
    }

    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append("[ListenerMetaData, ");
        b.append(super.toString());
        b.append(", bindAddress=");
        b.append(bindAddress);
        b.append(", port=");
        b.append(port);
        b.append(", hostAccessList=");
        b.append(hostAccessList);
        b.append("]");
        return b.toString();
    }
}

