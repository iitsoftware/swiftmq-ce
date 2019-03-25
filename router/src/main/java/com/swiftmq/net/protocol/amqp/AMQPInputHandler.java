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

package com.swiftmq.net.protocol.amqp;

import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;
import com.swiftmq.swiftlet.trace.TraceSpace;

import java.nio.ByteBuffer;

public class AMQPInputHandler implements ProtocolInputHandler {
    ChunkListener listener = null;
    int initialSize = 0;
    int ensureSize = 0;
    byte[] buffer = null;
    ByteBuffer byteBuffer = null;
    int writePos = 0;
    int readPos = 0;
    int length = -1;
    boolean mode091 = false;
    volatile int minLength = 4;
    volatile boolean protHeaderExpected = true;
    volatile String tracePrefix = null;
    volatile String traceKey = null;
    volatile TraceSpace traceSpace = null;

    public ProtocolInputHandler create() {
        AMQPInputHandler handler = new AMQPInputHandler();
        handler.setTraceKey(traceKey);
        handler.setTracePrefix(tracePrefix);
        handler.setTraceSpace(traceSpace);
        return handler;
    }

    private int readInt(byte[] b, int offset) {
        int pos = offset;
        int i1 = b[pos++] & 0xff;
        int i2 = b[pos++] & 0xff;
        int i3 = b[pos++] & 0xff;
        int i4 = b[pos++] & 0xff;
        int i = (i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0);
        return i;
    }

    public void setTraceSpace(TraceSpace traceSpace) {
        this.traceSpace = traceSpace;
    }

    public void setTraceKey(String traceKey) {
        this.traceKey = traceKey;
    }

    public void setTracePrefix(String tracePrefix) {
        this.tracePrefix = tracePrefix;
    }

    public void setProtHeaderExpected(boolean protHeaderExpected) {
        this.protHeaderExpected = protHeaderExpected;
    }

    public void setMode091(boolean mode091) {
        this.mode091 = mode091;
        if (mode091)
            minLength = 7;
    }

    public void setChunkListener(ChunkListener listener) {
        this.listener = listener;
    }

    public void createInputBuffer(int initialSize, int ensureSize) {
        this.initialSize = initialSize;
        this.ensureSize = ensureSize;
        buffer = new byte[initialSize];
        byteBuffer = ByteBuffer.wrap(buffer);
    }

    public ByteBuffer getByteBuffer() {
        getBuffer();
        byteBuffer.position(writePos);
        return byteBuffer;
    }

    public byte[] getBuffer() {
        if (buffer.length - writePos < ensureSize) {
            byte[] b = null;
            int available = readPos + buffer.length - writePos;
            if (available >= ensureSize) {
                // compact buffer
                b = new byte[buffer.length];
            } else {
                // extend buffer
                b = new byte[buffer.length + ensureSize];
            }
            System.arraycopy(buffer, readPos, b, 0, writePos - readPos);
            buffer = b;
            writePos -= readPos;
            readPos = 0;
            byteBuffer = ByteBuffer.wrap(buffer);
        }
        return buffer;
    }

    public int getOffset() {
        return writePos;
    }

    public void setBytesWritten(int written) {
        if (written == 0)
            return;
        writePos += written;
        int available = writePos - readPos;
        boolean moreDataBuffered = false;
        if (traceSpace != null && traceSpace.enabled)
            traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/1, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);
        do {
            moreDataBuffered = false;
            if (protHeaderExpected) {
                if (available >= 8) {
                    listener.chunkCompleted(buffer, readPos, 8);
                    readPos += 8;
                    available -= 8;
                    moreDataBuffered = available > 0;
                    if (traceSpace != null && traceSpace.enabled)
                        traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/2, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);
                }
            } else if (length == -1) {
                if (available >= minLength) {
                    length = mode091 ? readInt(buffer, readPos + 3) + 8 : readInt(buffer, readPos);
                    if (available >= length) {
                        listener.chunkCompleted(buffer, readPos, length);
                        readPos += length;
                        available -= length;
                        length = -1;
                        moreDataBuffered = available > 0;
                        if (traceSpace != null && traceSpace.enabled)
                            traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/3, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);
                    }
                }
            } else if (available >= length) {
                listener.chunkCompleted(buffer, readPos, length);
                readPos += length;
                available -= length;
                length = -1;
                moreDataBuffered = available > 0;
                if (traceSpace != null && traceSpace.enabled)
                    traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/4, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);
            }
            if (traceSpace != null && traceSpace.enabled)
                traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/5, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);
            if (readPos == writePos) {
                readPos = 0;
                writePos = 0;
                moreDataBuffered = false;
            }
        } while (moreDataBuffered);
        if (traceSpace != null && traceSpace.enabled)
            traceSpace.trace(traceKey, tracePrefix + "/" + toString() + "/6, setBytesWritten, written=" + written + ", writePos=" + writePos + ", readPos=" + readPos + ", available=" + available + ", moreDataBuffered=" + moreDataBuffered + ", protHeaderExpected=" + protHeaderExpected);

    }

    public String toString() {
        return "AMQPInputHandler, mode091=" + mode091;
    }
}
