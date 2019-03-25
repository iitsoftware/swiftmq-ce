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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

public class DataByteArrayOutputStream extends OutputStream implements DataOutput {
    static final int BUFFER_SIZE = 8192;

    byte[] buffer = null;
    int count = 0;
    int extendSize = BUFFER_SIZE;

    public DataByteArrayOutputStream() {
        this(BUFFER_SIZE, BUFFER_SIZE);
    }

    public DataByteArrayOutputStream(int bufferSize) {
        this(bufferSize, bufferSize);
    }

    public DataByteArrayOutputStream(int bufferSize, int extendSize) {
        buffer = new byte[bufferSize];
        this.extendSize = extendSize;
    }

    public DataByteArrayOutputStream(byte[] buffer) {
        this.buffer = buffer;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getCount() {
        return count;
    }

    private void ensure(int size) {
        if (buffer.length < count + size) {
            byte[] b = new byte[buffer.length + Math.max(size - (buffer.length - count), extendSize)];
            System.arraycopy(buffer, 0, b, 0, count);
            buffer = b;
        }
    }

    public void write(int b) throws IOException {
        ensure(1);
        buffer[count++] = (byte) b;
    }

    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte b[], int off, int len) throws IOException {
        ensure(len);
        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    public void writeBoolean(boolean v) throws IOException {
        ensure(1);
        write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        ensure(2);
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public void writeChar(int v) throws IOException {
        ensure(2);
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public void writeInt(int v) throws IOException {
        ensure(4);
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v) & 0xFF);
    }

    public void writeLong(long v) throws IOException {
        ensure(8);
        write((int) (v >>> 56) & 0xFF);
        write((int) (v >>> 48) & 0xFF);
        write((int) (v >>> 40) & 0xFF);
        write((int) (v >>> 32) & 0xFF);
        write((int) (v >>> 24) & 0xFF);
        write((int) (v >>> 16) & 0xFF);
        write((int) (v >>> 8) & 0xFF);
        write((int) (v) & 0xFF);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {
        int len = s.length();
        ensure(len);
        for (int i = 0; i < len; i++)
            write((byte) s.charAt(i));
    }

    public void writeChars(String s) throws IOException {
        int len = s.length();
        ensure(len * 2);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v) & 0xFF);
        }
    }

    public void writeUTF(String str) throws IOException {
        int utfCount = UTFUtils.countUTFBytes(str);
        if (utfCount > 65535)
            throw new UTFDataFormatException();
        ensure((int) utfCount + 2);
        count = UTFUtils.writeShortToBuffer((int) utfCount, buffer, count);
        count = UTFUtils.writeUTFBytesToBuffer(str, buffer, count);
    }

    public void rewind() {
        count = 0;
    }
}
