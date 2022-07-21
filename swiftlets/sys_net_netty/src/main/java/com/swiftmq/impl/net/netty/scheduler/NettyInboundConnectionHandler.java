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

import com.swiftmq.impl.net.netty.Countable;
import com.swiftmq.impl.net.netty.SwiftletContext;
import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class NettyInboundConnectionHandler extends ChannelInboundHandlerAdapter implements ChunkListener, TimerListener {
    SwiftletContext ctx;
    Connection connection;
    ProtocolInputHandler inputHandler = null;
    InboundHandler inboundHandler = null;
    DataByteArrayInputStream bais = null;
    Countable countableInput;
    boolean closed = false;
    volatile boolean zombi = true;
 
    public NettyInboundConnectionHandler(SwiftletContext ctx, Connection connection) {
        this.ctx = ctx;
        this.connection = connection;
        inputHandler = connection.getMetaData().createProtocolInputHandler();
        connection.setProtocolInputHandler(inputHandler);
        inputHandler.setChunkListener(this);
        inputHandler.createInputBuffer(connection.getMetaData().getInputBufferSize(), connection.getMetaData().getInputExtendSize());
        inboundHandler = connection.getInboundHandler();
        countableInput = (Countable)connection.getInputStream();
        long zombiConnectionTimeout = ctx.networkSwiftlet.getZombiConnectionTimeout();
        if (zombiConnectionTimeout > 0)
            ctx.timerSwiftlet.addInstantTimerListener(zombiConnectionTimeout, this);
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        ctx.logSwiftlet.logInformation("sys$net", toString()+"/connection inactive, closing");
        ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
        closed = true;

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
        ctx.logSwiftlet.logInformation("sys$net", toString()+"/Got exception: "+cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {
        byte[] buffer = inputHandler.getBuffer();
        int offset = inputHandler.getOffset();
        ByteBuf in = (ByteBuf) msg;
        int readableBytes = in.readableBytes();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/channelRead, readableBytes: "+readableBytes);
        try {
            in.readBytes(buffer, offset, readableBytes);
            inputHandler.setBytesWritten(readableBytes);
            countableInput.addByteCount(readableBytes);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$net", toString() + "/perform time action: checking for zombi connections...");
        if (zombi) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/zombi connection detected, close!");
            ctx.logSwiftlet.logWarning("sys$net", toString() + "/zombi connection detected, close! Please check for possible denial-of-service attack!");
            ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
            closed = true;
        }
    }

    @Override
    public void chunkCompleted(byte[] b, int offset, int len) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/chunk completed");
        try {
            long maxChunkSize = ctx.maxChunkSize.get();
            if (maxChunkSize != -1 && len > maxChunkSize)
                throw new Exception("Input message size (" + len + ") exceeds max-chunk-size (" + ctx.maxChunkSize.get() + ")");
            if (bais == null) {
                zombi = false;
                bais = new DataByteArrayInputStream();
            }
            bais.setBuffer(b, offset, len);
            inboundHandler.dataAvailable(connection, bais);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/Exception, EXITING: " + e);
            ctx.logSwiftlet.logInformation(toString(), "Exception, EXITING: " + e);
            if (!closed) {
                ctx.networkSwiftlet.getConnectionManager().removeConnection(connection);
                closed = true;
            }
        }
        
    }

    @Override
    public String toString() {
        return connection.toString();
    }
}
