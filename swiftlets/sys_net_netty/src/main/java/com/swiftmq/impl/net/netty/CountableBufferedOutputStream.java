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

package com.swiftmq.impl.net.netty;

import com.swiftmq.net.protocol.OutputListener;
import com.swiftmq.net.protocol.ProtocolOutputHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class CountableBufferedOutputStream extends OutputStream
        implements Countable, OutputListener {
    SocketChannel channel;
    ProtocolOutputHandler protocolOutputHandler = null;
    final AtomicLong byteCount = new AtomicLong();

    public CountableBufferedOutputStream(ProtocolOutputHandler protocolOutputHandler, SocketChannel channel) {
        this.protocolOutputHandler = protocolOutputHandler;
        this.channel = channel;
        protocolOutputHandler.setOutputListener(this);
    }

    public void write(byte[] b, int offset, int len) throws IOException {
        byteCount.addAndGet(len);
        protocolOutputHandler.write(b, offset, len);
    }

    public void write(int b) throws IOException {
        byteCount.getAndIncrement();
        protocolOutputHandler.write(b);
    }

    public void flush() throws IOException {
        protocolOutputHandler.flush();
        protocolOutputHandler.invokeOutputListener();
    }

    public int performWrite(byte[] b, int offset, int len)
            throws IOException {
        ByteBuf buffer = Unpooled.buffer(len);
        buffer.writeBytes(b, offset, len);
        channel.writeAndFlush(buffer);
        return len;
    }

    public void addByteCount(long cnt) {
        byteCount.addAndGet(cnt);
    }

    public long getByteCount() {
        return byteCount.get();
    }

    public void resetByteCount() {
        byteCount.set(0);
    }
}

