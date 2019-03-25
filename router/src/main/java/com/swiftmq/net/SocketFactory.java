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

package com.swiftmq.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Base interface for SwiftMQ socket factories.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2004, All Rights Reserved
 */
public interface SocketFactory {

    /**
     * Create a socket.
     *
     * @param host host.
     * @param port port.
     * @return new socket.
     * @throws UnknownHostException if the host is unknown.
     * @throws IOException          on error.
     */
    public Socket createSocket(String host, int port)
            throws UnknownHostException, IOException;

    /**
     * Create a socket.
     *
     * @param host          host.
     * @param port          port.
     * @param useTcpNoDelay use TCP No Delay.
     * @return new socket.
     * @throws UnknownHostException if the host is unknown.
     * @throws IOException          on error.
     */
    public Socket createSocket(String host, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException;

    /**
     * Create a socket.
     *
     * @param addr address.
     * @param port port.
     * @return new socket.
     * @throws UnknownHostException if the host is unknown.
     * @throws IOException          on error.
     */
    public Socket createSocket(InetAddress addr, int port)
            throws UnknownHostException, IOException;

    /**
     * Create a socket.
     *
     * @param addr          address.
     * @param port          port.
     * @param useTcpNoDelay use TCP No Delay.
     * @return new socket.
     * @throws UnknownHostException if the host is unknown.
     * @throws IOException          on error.
     */
    public Socket createSocket(InetAddress addr, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException;

    /**
     * Create a server socket.
     *
     * @param port port.
     * @return new server socket.
     * @throws IOException on error.
     */
    public ServerSocket createServerSocket(int port) throws IOException;

    /**
     * Create a server socket.
     *
     * @param port    port.
     * @param backlog the backlog (max. pending requests).
     * @return new server socket.
     * @throws IOException on error.
     */
    public ServerSocket createServerSocket(int port,
                                           int backlog) throws IOException;

    /**
     * Create a server socket.
     *
     * @param port     port.
     * @param backlog  the backlog (max. pending requests).
     * @param bindAddr address to bind the server socket to (for multiple network cards).
     * @return new server socket.
     * @throws IOException on error.
     */
    public ServerSocket createServerSocket(int port, int backlog,
                                           InetAddress bindAddr) throws IOException;
}



