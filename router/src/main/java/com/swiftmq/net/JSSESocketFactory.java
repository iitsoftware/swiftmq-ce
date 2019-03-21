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
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;

/**
 * A socket factory that creates JSSE (SSL) sockets. It uses Http tunneling automatically.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class JSSESocketFactory implements com.swiftmq.net.SocketFactory, Serializable {
    static final int SO_TIMEOUT = Integer.parseInt(SystemProperties.get("swiftmq.socket.sotimeout", "0"));
    static final boolean SET_SECURITY_PROVIDER = Boolean.valueOf(System.getProperty("swiftmq.jsse.set.security.provider", "false")).booleanValue();
    static final String SECURITY_PROVIDER = System.getProperty("swiftmq.jsse.security.provider", "com.sun.net.ssl.internal.ssl.Provider");

    static {
        // An alternative to do this here is a definition as security provider
        // in the java.security file of the jre
        if (SET_SECURITY_PROVIDER && Security.getProvider(SECURITY_PROVIDER) == null) {
            try {
                Security.addProvider((Provider) Class.forName(SECURITY_PROVIDER).newInstance());
            } catch (Exception e) {
                System.err.println("JSSESocketFactory: Exception Security.addProvider(\"" + SECURITY_PROVIDER + "\"): " + e);
            }
        }
    }

    protected static SSLSocketFactory getSocketFactory() throws IOException {
        SSLSocketFactory ssf = (SSLSocketFactory) SSLSocketFactory.getDefault();
        return ssf;
    }

    protected static ServerSocketFactory getServerSocketFactory() throws IOException {
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        return ssf;
    }

    private String[] getSupportedAnonCipherSuites(SSLSocket socket) {
        String[] supported = socket.getSupportedCipherSuites();
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < supported.length; i++)
            if (supported[i].startsWith("TLS_") && supported[i].contains("_anon_"))
                list.add(supported[i]);
        return (String[]) list.toArray(new String[list.size()]);
    }

    private String[] getSupportedAnonCipherSuites(SSLServerSocket socket) {
        String[] supported = socket.getSupportedCipherSuites();
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < supported.length; i++)
            if (supported[i].startsWith("TLS_") && supported[i].contains("_anon_"))
                list.add(supported[i]);
        return (String[]) list.toArray(new String[list.size()]);
    }

    protected void setEnabledCipherSuites(SSLSocket socket) throws IOException {
        boolean b = Boolean.valueOf(SystemProperties.get("swiftmq.jsse.anoncipher.enabled", "true")).booleanValue();
        if (b) {
            String anonCipherName = SystemProperties.get("swiftmq.jsse.anoncipher.name");
            if (anonCipherName != null)
                socket.setEnabledCipherSuites(new String[]{anonCipherName});
            else
                socket.setEnabledCipherSuites(getSupportedAnonCipherSuites(socket));
        }
    }

    protected void setEnabledCipherSuites(SSLServerSocket socket) throws IOException {
        boolean b = Boolean.valueOf(SystemProperties.get("swiftmq.jsse.anoncipher.enabled", "true")).booleanValue();
        if (b) {
            String anonCipherName = SystemProperties.get("swiftmq.jsse.anoncipher.name");
            if (anonCipherName != null)
                socket.setEnabledCipherSuites(new String[]{anonCipherName});
            else
                socket.setEnabledCipherSuites(getSupportedAnonCipherSuites(socket));
        }
        socket.setNeedClientAuth(Boolean.valueOf(SystemProperties.get("swiftmq.jsse.clientauth.enabled", "false")).booleanValue());
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
        setEnabledCipherSuites(sslSocket);
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
        setEnabledCipherSuites(sslSocket);
        Semaphore sem = new Semaphore();
        sslSocket.addHandshakeCompletedListener(new CompletionListener(sem));
        sslSocket.startHandshake();
        sem.waitHere();
        return sslSocket;
    }

    public Socket createSocket(InetAddress addr, int port)
            throws UnknownHostException, IOException {
        return createSocket(addr, port, true);
    }

    public ServerSocket createServerSocket(int port) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port);
        setEnabledCipherSuites(socket);
        return socket;
    }

    public ServerSocket createServerSocket(int port,
                                           int backlog) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port, backlog);
        setEnabledCipherSuites(socket);
        return socket;
    }

    public ServerSocket createServerSocket(int port, int backlog,
                                           InetAddress bindAddr) throws IOException {
        SSLServerSocket socket = (SSLServerSocket) getServerSocketFactory().createServerSocket(port, backlog, bindAddr);
        setEnabledCipherSuites(socket);
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



