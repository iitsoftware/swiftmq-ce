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

import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.prop.SystemProperties;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.io.IOException;
import java.io.Serializable;
import java.net.*;

/**
 * A socket factory that creates JSSE (SSL) sockets. It uses Http tunneling automatically.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class JSSESocketFactory implements com.swiftmq.net.SocketFactory, Serializable {
    static final int SO_TIMEOUT = Integer.parseInt(SystemProperties.get("swiftmq.socket.sotimeout", "0"));

    protected static SSLSocketFactory getSocketFactory() throws IOException {
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        return ssf;
    }

    protected static ServerSocketFactory getServerSocketFactory() throws IOException {
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        return ssf;
    }

    public Socket createSocket(String host, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException {
        SSLSocket sslSocket = null;
        if (HttpTunnelProperties.getInstance().isProxy() &&
                HttpTunnelProperties.getInstance().isHostViaProxy(host)) {
            Socket socket = new Socket(HttpTunnelProperties.getInstance().getProxyHost(), HttpTunnelProperties.getInstance().getProxyPort());
            HttpTunnelProperties.getInstance().setupHttpProxy(host, port, socket.getInputStream(), socket.getOutputStream());
            sslSocket = (SSLSocket) getSocketFactory().createSocket(socket, host, port, true);
        } else
            sslSocket = (SSLSocket) getSocketFactory().createSocket(host, port);
        if (SO_TIMEOUT > 0) {
            try {
                sslSocket.setSoTimeout(SO_TIMEOUT);
            } catch (SocketException e) {
                System.err.println("JSSESocketFactory: Unable to perform 'socket.setSoTimeout(" + SO_TIMEOUT + ")', exception: " + e);
            }
        }
        sslSocket.setTcpNoDelay(useTcpNoDelay);
        Semaphore sem = new Semaphore();
        sslSocket.addHandshakeCompletedListener(new CompletionListener(sem));
        sslSocket.startHandshake();
        sem.waitHere();
        return sslSocket;
    }

    public Socket createSocket(String host, int port)
            throws UnknownHostException, IOException {
        return createSocket(host, port, true);
    }

    public Socket createSocket(InetAddress addr, int port, boolean useTcpNoDelay)
            throws UnknownHostException, IOException {
        SSLSocket sslSocket = null;
        if (HttpTunnelProperties.getInstance().isProxy() &&
                HttpTunnelProperties.getInstance().isHostViaProxy(addr.getHostName())) {
            Socket socket = new Socket(HttpTunnelProperties.getInstance().getProxyHost(), HttpTunnelProperties.getInstance().getProxyPort());
            HttpTunnelProperties.getInstance().setupHttpProxy(addr.getHostName(), port, socket.getInputStream(), socket.getOutputStream());
            sslSocket = (SSLSocket) getSocketFactory().createSocket(socket, addr.getHostName(), port, true);
        } else
            sslSocket = (SSLSocket) getSocketFactory().createSocket(addr, port);
        if (SO_TIMEOUT > 0) {
            try {
                sslSocket.setSoTimeout(SO_TIMEOUT);
            } catch (SocketException e) {
                System.err.println("JSSESocketFactory: Unable to perform 'socket.setSoTimeout(" + SO_TIMEOUT + ")', exception: " + e);
            }
        }
        sslSocket.setTcpNoDelay(useTcpNoDelay);
        Semaphore sem = new Semaphore();
        sslSocket.addHandshakeCompletedListener(new CompletionListener(sem));
        sslSocket.startHandshake();
        sem.waitHere();
        return sslSocket;
    }

    public Socket createSocket(InetAddress addr, int port)
            throws IOException {
        return createSocket(addr, port, true);
    }

    public ServerSocket createServerSocket(int port) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port);
        return socket;
    }

    public ServerSocket createServerSocket(int port,
                                           int backlog) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port, backlog);
        return socket;
    }

    public ServerSocket createServerSocket(int port, int backlog,
                                           InetAddress bindAddr) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port, backlog, bindAddr);
        return socket;
    }

    private class CompletionListener implements HandshakeCompletedListener {
        Semaphore sem = null;

        CompletionListener(Semaphore sem) {
            this.sem = sem;
        }

        public void handshakeCompleted(HandshakeCompletedEvent event) {
            sem.notifySingleWaiter();
        }
    }
}



