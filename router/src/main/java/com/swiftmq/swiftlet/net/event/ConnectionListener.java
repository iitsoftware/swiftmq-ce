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

package com.swiftmq.swiftlet.net.event;

import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ConnectionVetoException;

/**
 * A ConnectionListener is registered at a meta data object. It will be called
 * after a connection is established.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface ConnectionListener {

    /**
     * A connection has been connected.
     * The listener should check Swiftlet specific requirements such as authorization,
     * should register an <code>InboundHandler</code>, and should do his set up work.
     * If the connection should not connect, for any reason, it should throw a
     * <code>ConnectionVetoException</code> which in turn closes the connection (without
     * calling the <code>disconnected()</code> method.
     *
     * @param connection connection.
     * @throws ConnectionVetoException if there is a veto.
     * @see com.swiftmq.swiftlet.net.InboundHandler
     */
    public void connected(Connection connection) throws ConnectionVetoException;


    /**
     * A connection has been disconnected.
     * Will be called from the connection manager before terminating the connection
     * physically. The listener should do his clean up work here.
     *
     * @param connection connection.
     */
    public void disconnected(Connection connection);
}

