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

public class DataStreamOutputStream extends OutputStream implements DataOutput {
    OutputStream outputStream = null;

    byte[] bytearr = new byte[8];

    public DataStreamOutputStream(OutputStream out) {
        this.outputStream = out;
    }

    public DataStreamOutputStream() {
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    public void write(byte b[], int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    public void write(byte b[]) throws IOException {
        outputStream.write(b, 0, b.length);
    }

    public void writeBoolean(boolean v) throws IOException {
        outputStream.write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        outputStream.write(v);
    }

    public void writeShort(int v) throws IOException {
        bytearr[0] = (byte) ((v >>> 8) & 0xFF);
        bytearr[1] = (byte) ((v) & 0xFF);
        outputStream.write(bytearr, 0, 2);
    }

    public void writeChar(int v) throws IOException {
        bytearr[0] = (byte) ((v >>> 8) & 0xFF);
        bytearr[1] = (byte) ((v) & 0xFF);
        outputStream.write(bytearr, 0, 2);
    }

    public void writeInt(int v) throws IOException {
        bytearr[0] = (byte) ((v >>> 24) & 0xFF);
        bytearr[1] = (byte) ((v >>> 16) & 0xFF);
        bytearr[2] = (byte) ((v >>> 8) & 0xFF);
        bytearr[3] = (byte) ((v) & 0xFF);
        outputStream.write(bytearr, 0, 4);
    }

    public void writeLong(long v) throws IOException {
        bytearr[0] = (byte) ((v >>> 56) & 0xFF);
        bytearr[1] = (byte) ((v >>> 48) & 0xFF);
        bytearr[2] = (byte) ((v >>> 40) & 0xFF);
        bytearr[3] = (byte) ((v >>> 32) & 0xFF);
        bytearr[4] = (byte) ((v >>> 24) & 0xFF);
        bytearr[5] = (byte) ((v >>> 16) & 0xFF);
        bytearr[6] = (byte) ((v >>> 8) & 0xFF);
        bytearr[7] = (byte) ((v) & 0xFF);
        outputStream.write(bytearr, 0, 8);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            outputStream.write((byte) s.charAt(i));
        }
    }

    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeChar(v);
        }
    }

    public final void writeUTF(String str) throws IOException {
        int utfCount = UTFUtils.countUTFBytes(str);
        if (utfCount > 65535)
            throw new UTFDataFormatException();
        byte[] buffer = new byte[(int) utfCount + 2];
        int offset = 0;
        offset = UTFUtils.writeShortToBuffer((int) utfCount, buffer, offset);
        offset = UTFUtils.writeUTFBytesToBuffer(str, buffer, offset);
        write(buffer, 0, offset);
    }

    public void flush() throws IOException {
        outputStream.flush();
    }

    public void close() throws IOException {
        outputStream.close();
    }
}
