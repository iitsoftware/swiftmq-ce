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

public class DataByteArrayInputStream extends InputStream implements LengthCaptureDataInput {
    byte[] buffer = null;
    int _offset = 0;
    int pos = 0;
    int max = 0;
    char[] strbuffer = new char[256];
    long captureLength = 0;

    public DataByteArrayInputStream() {
    }

    public DataByteArrayInputStream(byte[] buffer) {
        this.buffer = buffer;
        pos = 0;
        max = buffer.length;
    }

    public DataByteArrayInputStream(DataByteArrayOutputStream dos) {
        byte[] b = dos.getBuffer();
        int cnt = dos.getCount();
        if (cnt > 0) {
            buffer = new byte[cnt];
            System.arraycopy(b, 0, buffer, 0, cnt);
        } else
            buffer = new byte[0];
        pos = 0;
        max = buffer.length;
    }

    public void startCaptureLength() {
        captureLength = 0;
    }

    public long stopCaptureLength() {
        return captureLength;
    }

    public void setBuffer(byte[] b) {
        setBuffer(b, 0, b.length);
    }

    public void setBuffer(byte[] b, int off) {
        setBuffer(b, off, b.length - off);
    }

    public void setBuffer(byte[] b, int off, int len) {
        this.buffer = b;
        this.pos = off;
        this.max = off + len;
        this._offset = off;
        this.captureLength = 0;
    }

    public int getMax() {
        return max;
    }

    public int available() throws IOException {
        return max - pos;
    }

    public int read() throws IOException {
        captureLength++;
        return (pos < max) ? (buffer[pos++] & 0xff) : -1;
    }

    public void readFully(byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte b[], int off, int len) throws IOException {
        if (len + pos <= max) {
            System.arraycopy(buffer, pos, b, off, len);
            pos += len;
            captureLength += len;
        } else
            throw new EOFException();
    }

    public int skipBytes(int n) throws IOException {
        int skip = Math.min(n, max - pos);
        pos += skip;
        captureLength += skip;
        return skip;
    }

    public boolean readBoolean() throws IOException {
        if (pos < max) {
            captureLength++;
            return buffer[pos++] != 0;
        }
        throw new EOFException();
    }

    public byte readByte() throws IOException {
        if (pos < max) {
            captureLength++;
            return buffer[pos++];
        }
        throw new EOFException();
    }

    public int readUnsignedByte() throws IOException {
        if (pos < max) {
            captureLength++;
            return buffer[pos++] & 0xff;
        }
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
        int utfLength = readUnsignedShort();
        String s = UTFUtils.convertFromUTF8(buffer, pos, utfLength);
        pos += utfLength;
        captureLength += utfLength;
        return s;
    }

    public void reset() throws IOException {
        pos = 0;
        captureLength = 0;
    }
}
