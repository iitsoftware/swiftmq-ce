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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataByteBufferInputStream extends InputStream implements LengthCaptureDataInput {
    ByteBuffer buffer = null;
    char[] strBuffer = new char[256];
    int startPos = 0;

    public DataByteBufferInputStream() {
    }

    public DataByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public DataByteBufferInputStream(DataByteBufferOutputStream dos) {
        buffer = dos.getBuffer();
        buffer.rewind();
    }

    public void startCaptureLength() {
        startPos = buffer.position();
    }

    public long stopCaptureLength() {
        return buffer.position() - startPos;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int getMax() {
        return buffer.limit();
    }

    public int available() throws IOException {
        return buffer.remaining();
    }

    public int read() throws IOException {
        return buffer.hasRemaining() ? buffer.get() & 0xff : -1;
    }

    public void readFully(byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte b[], int off, int len) throws IOException {
        if (len <= buffer.remaining())
            buffer.get(b, off, len);
        else
            throw new EOFException();
    }

    public int skipBytes(int n) throws IOException {
        int skip = Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + skip);
        return skip;
    }

    public boolean readBoolean() throws IOException {
        if (buffer.hasRemaining())
            return buffer.get() != 0;
        throw new EOFException();
    }

    public byte readByte() throws IOException {
        if (buffer.hasRemaining())
            return buffer.get();
        throw new EOFException();
    }

    public int readUnsignedByte() throws IOException {
        if (buffer.hasRemaining())
            return buffer.get() & 0xff;
        throw new EOFException();
    }

    public short readShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short) ((ch1 << 8) + (ch2));
    }

    public int readUnsignedShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return ((ch1 << 8) + (ch2));
    }

    public char readChar() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char) ((ch1 << 8) + (ch2));
    }

    public int readInt() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    public long readLong() throws IOException {
        return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        return null;
    }

    public String readUTF() throws IOException {
        return UTFUtils.decodeUTF(readUnsignedShort(), this);
    }

    public void reset() throws IOException {
        buffer.rewind();
    }
}