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

import com.swiftmq.net.client.IntraVMConnection;
import com.swiftmq.swiftlet.Swiftlet;

/**
 * The Network Swiftlet serves as entry point to the network for a SwiftMQ router.
 * It is able to create TCP listeners and connectors, as well as multicast connections.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class NetworkSwiftlet extends Swiftlet {
    ConnectionManager connectionManager;

    /**
     * Returns whether server sockets should be re-used after a reboot of a router
     *
     * @return true/false
     */
    public abstract boolean isReuseServerSocket();

    /**
     * Returns whether DNS resolution is enabled
     *
     * @return true/false
     */
    public abstract boolean isDnsResolve();


    /**
     * Create a TCP listener.
     * The listener is specified through the meta data. Incoming connections on this listeners
     * are passed to a <code>ConnectionListener</code> which decides to accept the connection.
     *
     * @param metaData listener meta data
     * @throws Exception on error.
     * @see ConnectionListener
     */
    public abstract void createTCPListener(ListenerMetaData metaData)
            throws Exception;


    /**
     * Removes a TCP listener.
     *
     * @param metaData listener meta data
     */
    public abstract void removeTCPListener(ListenerMetaData metaData);


    /**
     * Create an intra-VM listener.
     * The listener is specified through the meta data. Incoming connections on this listeners
     * are passed to a <code>ConnectionListener</code> which decides to accept the connection.
     *
     * @param metaData intra-VM listener meta data
     * @throws Exception on error.
     * @see ConnectionListener
     */
    public abstract void createIntraVMListener(IntraVMListenerMetaData metaData)
            throws Exception;

    /**
     * Removes a intra-VM listener.
     *
     * @param metaData intra-VM listener meta data
     */
    public abstract void removeIntraVMListener(IntraVMListenerMetaData metaData);

    /**
     * Connects to an intra-VM listener.
     *
     * @param swiftletName swiftlet name
     * @param connection   intra-VM client connection
     * @throws Exception on error.
     */
    public abstract void connectIntraVMListener(String swiftletName, IntraVMConnection connection)
            throws Exception;

    /**
     * Create a TCP connector.
     * A connector is a single outgoing connection, specified by the meta data. A connector
     * has a retry interval. If a connection can't be established, further attempts in the retry
     * interval will be made. After a TCP connection is established, the <code>ConnectionListener</code>
     * will be informed to accept the connection.
     *
     * @param metaData connector meta data.
     * @throws Exception on error.
     * @see ConnectionListener
     */
    public abstract void createTCPConnector(ConnectorMetaData metaData)
            throws Exception;


    /**
     * Removes a TCP connector
     *
     * @param metaData connector meta data.
     */
    public abstract void removeTCPConnector(ConnectorMetaData metaData);


    /**
     * Returns the connection manager
     *
     * @return connection manager.
     */
    public ConnectionManager getConnectionManager() {
        // SBgen: Get variable
        return (connectionManager);
    }


    /**
     * Set the connection manager
     *
     * @param connectionManager connection manager.
     */
    protected void setConnectionManager(ConnectionManager connectionManager) {
        // SBgen: Assign variable
        this.connectionManager = connectionManager;
    }
}

