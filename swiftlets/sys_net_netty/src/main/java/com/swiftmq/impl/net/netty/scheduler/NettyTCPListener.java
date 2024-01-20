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
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.swiftlet.net.event.ConnectionListener;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class NettyTCPListener extends TCPListener {
    SwiftletContext ctx;
    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    TaskExecutor taskExecutor;
    final AtomicReference<ChannelFuture> channelFuture = new AtomicReference<>();
    boolean useTLS = false;

    public NettyTCPListener(SwiftletContext ctx, ListenerMetaData metaData) {
        super(metaData);
        this.ctx = ctx;
        useTLS = metaData.getSocketFactoryClass().equals("com.swiftmq.net.JSSESocketFactory");
        taskExecutor = new TaskExecutor(ctx);
        bossGroup = new NioEventLoopGroup(1, taskExecutor);
        workerGroup = new NioEventLoopGroup(taskExecutor.getNumberThreads()-1, taskExecutor);
    }

    private NettyConnection setupConnection(SocketChannel ch) throws Exception {
        if (ctx.networkSwiftlet.isDnsResolve() && !getMetaData().isConnectionAllowed(ch.remoteAddress().getHostName())) {
            String msg = "connection NOT allowed, REJECTED: " + ch.remoteAddress().getHostName();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/" + msg);
            ctx.logSwiftlet.logError(toString(), msg);
            throw new Exception(msg);
        }
        NettyConnection connection = new NettyConnection(ctx, ch, ctx.networkSwiftlet.isDnsResolve());
        ConnectionListener connectionListener = getMetaData().getConnectionListener();
        connection.setConnectionListener(connectionListener);
        connection.setMetaData(getMetaData());
        connectionListener.connected(connection);
        ctx.networkSwiftlet.getConnectionManager().addConnection(connection);
        ctx.logSwiftlet.logInformation("sys$net", super.toString() + ", connection accepted: " + connection.toString());
        return connection;
    }

    @Override
    public void start() throws IOException {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
                            ctx.logSwiftlet.logInformation("sys$net", NettyTCPListener.this.toString()+"/Got exception: "+cause);
                        }

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            if (useTLS)
                                ch.pipeline().addLast(SSLContextFactory.createServerContext().newHandler(ch.alloc()));
                            ch.pipeline().addLast(new NettyInboundConnectionHandler(ctx, setupConnection(ch)));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 500)
                    .option(ChannelOption.SO_REUSEADDR, ctx.networkSwiftlet.isReuseServerSocket())
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_SNDBUF, getMetaData().getOutputBufferSize())
                    .childOption(ChannelOption.SO_RCVBUF, getMetaData().getInputBufferSize())
                    .childOption(ChannelOption.TCP_NODELAY, getMetaData().isUseTcpNoDelay());

            if (getMetaData().getBindAddress() != null)
                channelFuture.set(b.bind(getMetaData().getBindAddress(), getMetaData().getPort()).sync());
            else
                channelFuture.set(b.bind(getMetaData().getPort()).sync());
        } catch (InterruptedException e) {
            throw new IOException(e.toString());
        }
    }

    @Override
    public void close() {
        ChannelFuture future = channelFuture.getAndSet(null);
        if (future != null)
            future.channel().close();
        try {
            workerGroup.shutdownGracefully().get();
            bossGroup.shutdownGracefully().get();
        } catch (Exception ignored) {
        }
    }
}
