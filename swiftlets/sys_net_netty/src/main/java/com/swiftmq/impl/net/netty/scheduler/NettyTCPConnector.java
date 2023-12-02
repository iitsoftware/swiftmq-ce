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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class NettyTCPConnector extends TCPConnector {
    EventLoopGroup group;
    TaskExecutor taskExecutor;
    final AtomicReference<ChannelFuture> channelFuture = new AtomicReference<>();
    final AtomicReference<NettyConnection> connection = new AtomicReference<>();
    final AtomicReference<NettyOutboundConnectionHandler> connectionHandler = new AtomicReference<>();
    boolean useTLS = false;

    public NettyTCPConnector(SwiftletContext ctx, ConnectorMetaData metaData) {
        super(ctx, metaData);
        useTLS = metaData.getSocketFactoryClass() != null && metaData.getSocketFactoryClass().equals("com.swiftmq.net.JSSESocketFactory");
        taskExecutor = new TaskExecutor(ctx);
        group = new NioEventLoopGroup(taskExecutor.getNumberThreads() - 1, taskExecutor);
    }

    @Override
    public synchronized void connect() {
        NettyOutboundConnectionHandler handler = connectionHandler.get();
        if (handler != null && handler.isActive())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/connect");
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
                connection.set(new NettyConnection(ctx, socketChannel, ctx.networkSwiftlet.isDnsResolve()));
                connectionHandler.set(new NettyOutboundConnectionHandler(ctx, connection.get(), getMetaData()));
                if (useTLS)
                    socketChannel.pipeline().addLast(SSLContextFactory.createClientContext().newHandler(socketChannel.alloc()));
                socketChannel.pipeline().addLast(connectionHandler.get());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$net", NettyTCPConnector.this.toString() + "/initChannel");
            }

            @Override
            public void channelInactive(ChannelHandlerContext context) throws Exception {
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
            channelFuture.set(clientBootstrap.connect().sync());
            ctx.logSwiftlet.logInformation("sys$net", super.toString() + ", connection created: " + connection.toString());
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", super.toString() + ", connect, exception: " + e.toString());
            channelFuture.set(null);
            connectionHandler.set(null);
            connection.set(null);
        }
    }

    @Override
    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", super.toString() + "/close");
        super.close();
        ChannelFuture future = channelFuture.getAndSet(null);
        if (future != null)
            future.channel().close();
        group.shutdownGracefully();
        connectionHandler.set(null);
        connection.set(null);
    }
}
