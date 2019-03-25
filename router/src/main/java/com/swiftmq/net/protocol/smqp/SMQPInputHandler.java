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

package com.swiftmq.net.protocol.smqp;

import com.swiftmq.net.protocol.ChunkListener;
import com.swiftmq.net.protocol.ProtocolInputHandler;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A SMQPInputHandler handles SMQP input.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class SMQPInputHandler implements ProtocolInputHandler {
    static final boolean debug = Boolean.valueOf(System.getProperty("swiftmq.smqp.handler.debug", "false")).booleanValue();
    static final SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy-HH:mm:ss.SSS");

    ChunkListener listener = null;
    int initialSize = 0;
    int ensureSize = 0;
    byte[] lengthField = new byte[4];
    byte[] buffer = null;
    ByteBuffer byteBuffer = null;
    int bufferOffset = 0;
    boolean lengthComplete = false;
    int lengthByteCount = 0;
    int chunkMissed = 0;
    int chunkLength = 0;
    int chunkStart = 0;
    int lengthFieldPos = 0;
    long gclen = 0;

    public ProtocolInputHandler create() {
        return new SMQPInputHandler();
    }

    public void setChunkListener(ChunkListener listener) {
        this.listener = listener;
    }

    public void createInputBuffer(int initialSize, int ensureSize) {
        this.initialSize = initialSize;
        this.ensureSize = ensureSize;
        buffer = new byte[initialSize];
        byteBuffer = ByteBuffer.wrap(buffer);
        if (debug)
            System.out.println(format.format(new Date()) + "/" + super.toString() + ", createInputBuffer, len=" + buffer.length);
    }

    public ByteBuffer getByteBuffer() {
        getBuffer();
        byteBuffer.position(bufferOffset);
        return byteBuffer;
    }

    public byte[] getBuffer() {
        if (buffer.length - bufferOffset < ensureSize) {
            gclen += buffer.length;
            byte[] b = new byte[buffer.length + ensureSize];
            System.arraycopy(buffer, 0, b, 0, bufferOffset);
            buffer = b;
            byteBuffer = ByteBuffer.wrap(buffer);
            if (debug)
                System.out.println(format.format(new Date()) + "/" + super.toString() + ", extend buffer, gced=" + gclen + ", len=" + buffer.length);
        }
        return buffer;
    }

    public int getOffset() {
        return bufferOffset;
    }

    private int readLength(byte[] b, int offset) {
        int i1 = b[offset] & 0xff;
        int i2 = b[offset + 1] & 0xff;
        int i3 = b[offset + 2] & 0xff;
        int i4 = b[offset + 3] & 0xff;
        int i = (i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0);
        return i;
    }

    public void setBytesWritten(int written) {
        if (lengthComplete) {
            if (written >= chunkMissed) {
                listener.chunkCompleted(buffer, chunkStart, chunkLength);
                written -= chunkMissed;
                bufferOffset += chunkMissed;
                lengthComplete = false;
                lengthByteCount = 0;
                chunkMissed = 0;
                if (written > 0) {
                    lengthFieldPos = chunkStart + chunkLength;
                    setBytesWritten(written);
                } else {
                    bufferOffset = 0;
                    lengthFieldPos = 0;
                }
            } else {
                bufferOffset += written;
                chunkMissed -= written;
            }
        } else {
            if (lengthByteCount + written >= 4) {
                int rest = 4 - lengthByteCount;
                chunkLength = readLength(buffer, lengthFieldPos);
                bufferOffset += rest;
                chunkStart = bufferOffset;
                chunkMissed = chunkLength;
                written -= rest;
                lengthByteCount = 4;
                lengthComplete = true;
                if (written > 0)
                    setBytesWritten(written);
            } else {
                bufferOffset += written;
                lengthByteCount += written;
            }
        }
    }

    public String toString() {
        return "[SMQPInputHandler]";
    }
}

