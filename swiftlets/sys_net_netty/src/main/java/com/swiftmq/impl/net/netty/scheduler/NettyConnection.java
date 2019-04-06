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

package com.swiftmq.impl.net.netty.scheduler;

import com.swiftmq.impl.net.netty.CountableBufferedOutputStream;
import com.swiftmq.impl.net.netty.CountableInput;
import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ConnectionMetaData;
import io.netty.channel.socket.SocketChannel;

import java.io.InputStream;
import java.io.OutputStream;

public class NettyConnection extends Connection {
    SwiftletContext ctx;
    SocketChannel channel;
    String hostname = null;
    int port = 0;
    InputStream in;
    OutputStream out;

    public NettyConnection(SwiftletContext ctx, SocketChannel channel, boolean dnsResolve) {
        super(dnsResolve);
        this.ctx = ctx;
        this.channel = channel;
        in = new CountableInput();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/created");
    }

    private void determineHostnamePort() {
        if (hostname != null)
            return;
        try {
            hostname = dnsResolve ? channel.remoteAddress().getHostName() : channel.remoteAddress().getAddress().toString();
            if (hostname != null && hostname.startsWith("/"))
                hostname = hostname.substring(1);
            port = channel.remoteAddress().getPort();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", "/exception determining hostname: "+e.toString());
            hostname = null;
            port = -1;
        }
    }

    public String getHostname() {
        determineHostnamePort();
        if (hostname == null)
            return "unresolvable";
        return hostname;
    }

    public int getPort() {
        determineHostnamePort();
        return port;
    }

    @Override
    public void setMetaData(ConnectionMetaData metaData) {
        super.setMetaData(metaData);
        setProtocolOutputHandler(metaData.createProtocolOutputHandler());
        out = new CountableBufferedOutputStream(getProtocolOutputHandler(), channel);
    }

    @Override
    public InputStream getInputStream() {
        return in;
    }

    @Override
    public OutputStream getOutputStream() {
        return out;
    }

    @Override
    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/close, isClosed="+isClosed());
        if (isClosed())
            return;
        super.close();
        channel.close();
    }
    public String toString() {
        return getHostname() + ":" + getPort();
    }
}
