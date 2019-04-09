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

import com.swiftmq.impl.net.netty.SSLContextFactory;
import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.swiftlet.net.ConnectorMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class NettyTCPConnector extends TCPConnector {
    EventLoopGroup group;
    TaskExecutor taskExecutor;
    ChannelFuture channelFuture = null;
    NettyConnection connection = null;
    NettyOutboundConnectionHandler connectionHandler = null;
    boolean useTLS = false;

    public NettyTCPConnector(SwiftletContext ctx, ConnectorMetaData metaData) {
        super(ctx, metaData);
    }

    private void registerConnection() throws Exception {
        useTLS = metaData.getSocketFactoryClass() != null && metaData.getSocketFactoryClass().equals("com.swiftmq.net.JSSESocketFactory");
        taskExecutor = new TaskExecutor(ctx);
        group = new NioEventLoopGroup(taskExecutor.getNumberThreads()-1, taskExecutor);
        ConnectionListener connectionListener = getMetaData().getConnectionListener();
        connection.setConnectionListener(connectionListener);
        connection.setMetaData(getMetaData());
        connectionListener.connected(connection);
        ctx.networkSwiftlet.getConnectionManager().addConnection(connection);
        ctx.logSwiftlet.logInformation(super.toString(), "connection created: " + connection.toString());
    }

    @Override
    public synchronized void connect() {
        if (connectionHandler != null && connectionHandler.isActive())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", super.toString() + "/connect");
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group);
        clientBootstrap.channel(NioSocketChannel.class);
        clientBootstrap.remoteAddress(new InetSocketAddress(getMetaData().getHostname(), getMetaData().getPort()));
        clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
                ctx.logSwiftlet.logError("sys$net", NettyTCPConnector.this.toString()+"/Got exception: "+cause);
            }

            protected void initChannel(SocketChannel socketChannel) throws Exception {
                connection = new NettyConnection(ctx, socketChannel, ctx.networkSwiftlet.isDnsResolve());
                connectionHandler = new NettyOutboundConnectionHandler(ctx, connection) {
                    @Override
                    protected void register() throws Exception {
                        registerConnection();
                    }
                };
                if (useTLS)
                    socketChannel.pipeline().addLast(SSLContextFactory.createClientContext().newHandler(socketChannel.alloc()));
                socketChannel.pipeline().addLast(connectionHandler);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$net", NettyTCPConnector.this.toString() + "/initChannel");
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) throws Exception {
                super.channelInactive(context);
                close();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$net", NettyTCPConnector.this.toString() + "/channelInactive");
            }
        })
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_SNDBUF, getMetaData().getOutputBufferSize())
        .option(ChannelOption.SO_RCVBUF, getMetaData().getInputBufferSize())
        .option(ChannelOption.TCP_NODELAY, getMetaData().isUseTcpNoDelay());
        try {
            channelFuture = clientBootstrap.connect().sync();
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", super.toString() + "/connect, exception: " + e.toString());
            channelFuture = null;
            connectionHandler = null;
            connection = null;
        }
    }

    @Override
    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", super.toString() + "/close");
        super.close();
        if (channelFuture != null)
            channelFuture.channel().close();
        group.shutdownGracefully();
        channelFuture = null;
        connectionHandler = null;
        connection = null;
    }
}
