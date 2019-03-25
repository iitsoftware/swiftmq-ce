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

package com.swiftmq.tools.util;

import com.swiftmq.swiftlet.trace.TraceSpace;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

public class DataByteBufferOutputStream implements DataOutput {
    ByteBuffer buffer = null;
    int initialSize;
    int extendSize;
    boolean direct;
    TraceSpace traceSpace = null;
    String tracePrefix = null;

    public DataByteBufferOutputStream(int initialSize, int extendSize, boolean direct) {
        this.initialSize = initialSize;
        this.extendSize = extendSize;
        this.direct = direct;
        buffer = direct ? ByteBuffer.allocateDirect(initialSize) : ByteBuffer.allocate(initialSize);
    }

    private void ensure(int size) {
        if (buffer.remaining() < size) {
            int ext;
            if (size >= extendSize)
                ext = size + extendSize;
            else
                ext = extendSize;
            ByteBuffer b = direct ? ByteBuffer.allocateDirect(buffer.capacity() + ext) : ByteBuffer.allocate(buffer.capacity() + ext);
            if (traceSpace != null && traceSpace.enabled)
                traceSpace.trace(tracePrefix, "DataByteBufferOutputStream, direct=" + direct + ", current buffersize=" + buffer.capacity() + ", ext=" + ext + ", new buffersize=" + b.capacity());
            buffer.flip();
            b.put(buffer);
            buffer = b;
        }
    }

    private void put(int b) {
        buffer.put((byte) b);
    }

    public void setTrace(TraceSpace traceSpace, String tracePrefix) {
        this.traceSpace = traceSpace;
        this.tracePrefix = tracePrefix;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getCount() {
        return buffer.position();
    }

    public void write(int b) throws IOException {
        ensure(1);
        put(b);
    }

    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte b[], int off, int len) throws IOException {
        ensure(len);
        buffer.put(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        ensure(1);
        put(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        ensure(2);
        put((v >>> 8) & 0xFF);
        put((v) & 0xFF);
    }

    public void writeChar(int v) throws IOException {
        ensure(2);
        put((v >>> 8) & 0xFF);
        put((v) & 0xFF);
    }

    public void writeInt(int v) throws IOException {
        ensure(4);
        buffer.putInt(v);
    }

    public void writeLong(long v) throws IOException {
        ensure(8);
        buffer.putLong(v);
    }

    public void writeFloat(float v) throws IOException {
        ensure(8);
        buffer.putFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        ensure(8);
        buffer.putDouble(v);
    }

    public void writeBytes(String s) throws IOException {
        int len = s.length();
        ensure(len);
        for (int i = 0; i < len; i++)
            put((byte) s.charAt(i));
    }

    public void writeChars(String s) throws IOException {
        int len = s.length();
        ensure(len * 2);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            put((v >>> 8) & 0xFF);
            put((v) & 0xFF);
        }
    }

    public void writeUTF(String s) throws IOException {
        int utfCount = UTFUtils.countUTFBytes(s);
        if (utfCount > 65535)
            throw new UTFDataFormatException();
        writeShort((int) utfCount);
        writeUTFBytesToBuffer(s);
    }


    void writeUTFBytesToBuffer(String str) throws IOException {
        int length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                write((byte) charValue);
            } else if (charValue <= 2047) {
                write((byte) (0xc0 | (0x1f & (charValue >> 6))));
                write((byte) (0x80 | (0x3f & charValue)));
            } else {
                write((byte) (0xe0 | (0x0f & (charValue >> 12))));
                write((byte) (0x80 | (0x3f & (charValue >> 6))));
                write((byte) (0x80 | (0x3f & charValue)));
            }
        }
    }

    public void rewind() {
        buffer.clear();
    }
}
