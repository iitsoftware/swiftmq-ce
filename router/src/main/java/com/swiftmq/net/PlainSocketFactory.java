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

import com.swiftmq.tools.prop.SystemProperties;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;

/**
 * A socket factory that creates plain sockets. It uses Http tunneling automatically.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class PlainSocketFactory implements SocketFactory2, Serializable {
    static final boolean SET_SOCKET_OPTIONS = Boolean.valueOf(SystemProperties.get("swiftmq.socket.set.options", "true")).booleanValue();
    static final boolean SET_REUSE_ADDRESS = Boolean.valueOf(SystemProperties.get("swiftmq.socket.set.reuseaddress", "false")).booleanValue();
    static final int MAX_RCVBUFSIZE = Integer.parseInt(SystemProperties.get("swiftmq.socket.max.receivebuffersize", "0"));
    static final int CONNECT_TIMEOUT = Integer.parseInt(SystemProperties.get("swiftmq.socket.connect.timeout", "5000"));
    int rcvBufSize = -1;

    public void setReceiveBufferSize(int size) {
        if (SET_SOCKET_OPTIONS) {
            if (MAX_RCVBUFSIZE > 0)
                rcvBufSize = Math.min(size, MAX_RCVBUFSIZE);
            else
                rcvBufSize = size;
        }
    }

    private Socket _createSocket(String host, int port) throws IOException {
        Socket socket = null;
        if (rcvBufSize == -1)
            socket = new Socket(host, port);
        else {
            socket = new Socket();
            try {
                socket.setReceiveBufferSize(rcvBufSize);
            } catch (SocketException e) {
                System.err.println("Unable to set socket receive buffer size to: " + rcvBufSize);
            }
            if (SET_REUSE_ADDRESS)
                socket.setReuseAddress(true);
            if (CONNECT_TIMEOUT > 0)
                socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT);
            else
                socket.connect(new InetSocketAddress(host, port));
        }
        return socket;
    }

    public Socket createSocket(String host, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException {
        Socket socket = null;
        if (HttpTunnelProperties.getInstance().isProxy() &&
                HttpTunnelProperties.getInstance().isHostViaProxy(host)) {
            socket = _createSocket(HttpTunnelProperties.getInstance().getProxyHost(), HttpTunnelProperties.getInstance().getProxyPort());
            HttpTunnelProperties.getInstance().setupHttpProxy(host, port, socket.getInputStream(), socket.getOutputStream());
        } else
            socket = _createSocket(host, port);
        try {
            socket.setTcpNoDelay(useTcpNoDelay);
        } catch (SocketException e) {
        }
        return socket;
    }

    public Socket createSocket(String host, int port)
            throws UnknownHostException, IOException {
        return createSocket(host, port, true);
    }

    public Socket createSocket(InetAddress addr, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException {
        Socket socket = null;
        if (HttpTunnelProperties.getInstance().isProxy() &&
                HttpTunnelProperties.getInstance().isHostViaProxy(addr.getHostName())) {
            socket = _createSocket(HttpTunnelProperties.getInstance().getProxyHost(), HttpTunnelProperties.getInstance().getProxyPort());
            HttpTunnelProperties.getInstance().setupHttpProxy(addr.getHostName(), port, socket.getInputStream(), socket.getOutputStream());
        } else
            socket = _createSocket(addr.getHostName(), port);
        try {
            socket.setTcpNoDelay(useTcpNoDelay);
        } catch (SocketException e) {
        }
        return socket;
    }

    public Socket createSocket(InetAddress addr, int port)
            throws UnknownHostException, IOException {
        return createSocket(addr, port, true);
    }

    public ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket ss = null;
        if (rcvBufSize == -1)
            ss = new ServerSocket(port);
        else {
            ss = new ServerSocket();
            try {
                ss.setReceiveBufferSize(rcvBufSize);
            } catch (SocketException e) {
            }
            if (SET_REUSE_ADDRESS)
                ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
        }
        return ss;
    }

    public ServerSocket createServerSocket(int port,
                                           int backlog) throws IOException {
        ServerSocket ss = null;
        if (rcvBufSize == -1)
            ss = new ServerSocket(port, backlog);
        else {
            ss = new ServerSocket();
            try {
                ss.setReceiveBufferSize(rcvBufSize);
            } catch (SocketException e) {
            }
            if (SET_REUSE_ADDRESS)
                ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port), backlog);
        }
        return ss;
    }

    public ServerSocket createServerSocket(int port, int backlog,
                                           InetAddress bindAddr) throws IOException {
        ServerSocket ss = null;
        if (rcvBufSize == -1)
            ss = new ServerSocket(port, backlog, bindAddr);
        else {
            ss = new ServerSocket();
            try {
                ss.setReceiveBufferSize(rcvBufSize);
            } catch (SocketException e) {
            }
            if (SET_REUSE_ADDRESS)
                ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(bindAddr, port), backlog);
        }
        return ss;
    }

}



