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

package com.swiftmq.jndi.v400;

public class JNDIInfo implements java.io.Serializable {
    String username;
    String password;
    String hostname;
    int port;
    String factory;
    long timeout;
    long keepalive;
    boolean intraVM = false;
    boolean reconnect = false;
    long reconnectDelay = 0;
    int maxRetries = 0;
    String hostname2 = null;
    int port2 = 0;
    boolean debug = false;
    boolean hasParameters = false;

    protected JNDIInfo(String username, String password, String hostname, int port, String factory, long timeout, boolean intraVM) {
        this(username, password, hostname, port, factory, timeout, 0, intraVM);
    }

    protected JNDIInfo(String username, String password, String hostname, int port, String factory, long timeout, long keepalive, boolean intraVM) {
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.factory = factory;
        this.timeout = timeout;
        this.keepalive = keepalive;
        this.intraVM = intraVM;
    }

    protected JNDIInfo(String username, String password, String hostname, int port, String factory, long timeout, long keepalive, boolean intraVM, boolean reconnect, long reconnectDelay, int maxRetries, String hostname2, int port2, boolean debug, boolean hasParameters) {
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.factory = factory;
        this.timeout = timeout;
        this.keepalive = keepalive;
        this.intraVM = intraVM;
        this.reconnect = reconnect;
        this.reconnectDelay = reconnectDelay;
        this.maxRetries = maxRetries;
        this.hostname2 = hostname2;
        this.port2 = port2;
        this.debug = debug;
        this.hasParameters = hasParameters;
    }

    public String getUsername() {
        return (username);
    }

    public String getPassword() {
        return (password);
    }

    public String getHostname() {
        return (hostname);
    }

    public int getPort() {
        return (port);
    }

    public String getFactory() {
        return (factory);
    }

    public long getTimeout() {
        return (timeout);
    }

    public long getKeepalive() {
        return keepalive;
    }

    public boolean isIntraVM() {
        return intraVM;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getHostname2() {
        return hostname2;
    }

    public int getPort2() {
        return port2;
    }

    public boolean isDebug() {
        return debug;
    }

    public String getProviderURL(String host, int port) {
        StringBuffer b = new StringBuffer("smqp://");
        if (username != null) {
            b.append(username);
            if (password != null) {
                b.append(":");
                b.append(password);
            }
            b.append("@");
        }
        if (host.equals("intravm"))
            b.append("intravm");
        else {
            b.append(host);
            b.append(":");
            b.append(port);
            if (hasParameters) {
                b.append("/");
                boolean semiRequired = false;
                if (factory != null) {
                    b.append("type=");
                    b.append(factory);
                    semiRequired = true;
                }
                if (timeout != 0) {
                    if (semiRequired)
                        b.append(";");
                    b.append("timeout=");
                    b.append(timeout);
                    semiRequired = true;
                }
                if (keepalive != 0) {
                    if (semiRequired)
                        b.append(";");
                    b.append("keepalive=");
                    b.append(keepalive);
                    semiRequired = true;
                }
            }
        }
        return b.toString();
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[JNDIInfo, username=");
        s.append(username);
        s.append(", password=");
        s.append(password);
        s.append(", hostname=");
        s.append(hostname);
        s.append(", port=");
        s.append(port);
        s.append(", factory=");
        s.append(factory);
        s.append(", timeout=");
        s.append(timeout);
        s.append(", keepalive=");
        s.append(keepalive);
        s.append(", intraVM=");
        s.append(intraVM);
        s.append(", reconnect=");
        s.append(reconnect);
        s.append(", reconnectDelay=");
        s.append(reconnectDelay);
        s.append(", maxRetries=");
        s.append(maxRetries);
        s.append(", hostname2=");
        s.append(hostname2);
        s.append(", port2=");
        s.append(port2);
        s.append(", debug=");
        s.append(debug);
        s.append(", hasParameters=");
        s.append(hasParameters);
        s.append("]");
        return s.toString();
    }
}

