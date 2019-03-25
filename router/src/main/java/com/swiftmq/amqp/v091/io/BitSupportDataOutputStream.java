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

package com.swiftmq.amqp.v091.io;

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

public class BitSupportDataOutputStream implements BitSupportDataOutput {
    DataOutput out = null;
    BitSet bitSet = null;
    int nBits = 0;

    public BitSupportDataOutputStream(DataOutput out) {
        this.out = out;
    }

    public void writeBit(boolean b) throws IOException {
        if (bitSet == null)
            bitSet = new BitSet(8);
        bitSet.set(nBits++, b);
        if (nBits == 8)
            bitFlush();
    }

    public void bitFlush() throws IOException {
        if (bitSet != null) {
            byte b = 0;
            for (int i = 0; i < nBits; i++) {
                if (bitSet.get(i))
                    b |= 1 << (i % 8);
            }
            out.writeByte(b);
            bitSet = null;
            nBits = 0;
        }
    }

    public void write(int b) throws IOException {
        bitFlush();
        out.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        bitFlush();
        out.write(b, off, len);
    }

    public void write(byte[] b) throws IOException {
        bitFlush();
        out.write(b);
    }

    public void writeBoolean(boolean v) throws IOException {
        bitFlush();
        out.writeBoolean(v);
    }

    public void writeByte(int v) throws IOException {
        bitFlush();
        out.writeByte(v);
    }

    public void writeShort(int v) throws IOException {
        bitFlush();
        out.writeShort(v);
    }

    public void writeChar(int v) throws IOException {
        bitFlush();
        out.writeChar(v);
    }

    public void writeInt(int v) throws IOException {
        bitFlush();
        out.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        bitFlush();
        out.writeLong(v);
    }

    public void writeFloat(float v) throws IOException {
        bitFlush();
        out.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        bitFlush();
        out.writeDouble(v);
    }

    public void writeBytes(String s) throws IOException {
        bitFlush();
        out.writeBytes(s);
    }

    public void writeChars(String s) throws IOException {
        bitFlush();
        out.writeChars(s);
    }

    public void writeUTF(String str) throws IOException {
        bitFlush();
        out.writeUTF(str);
    }
}
