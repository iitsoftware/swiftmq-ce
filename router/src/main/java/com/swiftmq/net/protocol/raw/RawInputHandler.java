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

package com.swiftmq.net.protocol.raw;

import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;

import java.nio.ByteBuffer;

/**
 * A RawInputHandler handles a raw byte stream and pass them to a chunk listener
 * on every call to <code>put()</code>.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class RawInputHandler implements ProtocolInputHandler {
    ChunkListener listener = null;
    int initialSize = 0;
    byte[] buffer = null;
    ByteBuffer byteBuffer = null;

    public ProtocolInputHandler create() {
        return new RawInputHandler();
    }

    public void setChunkListener(ChunkListener listener) {
        this.listener = listener;
    }

    public void createInputBuffer(int initialSize, int ensureSize) {
        this.initialSize = initialSize;
        buffer = new byte[initialSize];
        byteBuffer = ByteBuffer.wrap(buffer);
    }

    public ByteBuffer getByteBuffer() {
        byteBuffer.rewind();
        return byteBuffer;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getOffset() {
        return 0;
    }

    public void setBytesWritten(int written) {
        listener.chunkCompleted(buffer, 0, written);
    }

    public String toString() {
        return "[RawInputHandler]";
    }
}
