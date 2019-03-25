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

import java.io.DataInput;
import java.io.IOException;
import java.util.BitSet;

public class BitSupportDataInputStream implements BitSupportDataInput {
    DataInput in;
    BitSet bitSet = null;
    int bitIndex = 0;

    public BitSupportDataInputStream(DataInput in) {
        this.in = in;
    }

    public boolean readBit() throws IOException {
        if (bitSet == null) {
            bitSet = new BitSet(8);
            byte b = in.readByte();
            for (int i = 0; i < 8; i++) {
                if ((b & (1 << (i % 8))) > 0)
                    bitSet.set(i);
            }
            bitIndex = 0;
        }
        boolean rc = bitSet.get(bitIndex++);
        if (bitIndex == 8)
            bitSet = null;
        return rc;
    }

    public void readFully(byte[] bytes) throws IOException {
        bitSet = null;
        in.readFully(bytes);
    }

    public void readFully(byte[] bytes, int i, int i1) throws IOException {
        bitSet = null;
        in.readFully(bytes, i, i1);
    }

    public int skipBytes(int i) throws IOException {
        bitSet = null;
        return in.skipBytes(i);
    }

    public boolean readBoolean() throws IOException {
        bitSet = null;
        return in.readBoolean();
    }

    public byte readByte() throws IOException {
        bitSet = null;
        return in.readByte();
    }

    public int readUnsignedByte() throws IOException {
        bitSet = null;
        return in.readUnsignedByte();
    }

    public short readShort() throws IOException {
        bitSet = null;
        return in.readShort();
    }

    public int readUnsignedShort() throws IOException {
        bitSet = null;
        return in.readUnsignedShort();
    }

    public char readChar() throws IOException {
        bitSet = null;
        return in.readChar();
    }

    public int readInt() throws IOException {
        bitSet = null;
        return in.readInt();
    }

    public long readLong() throws IOException {
        bitSet = null;
        return in.readLong();
    }

    public float readFloat() throws IOException {
        bitSet = null;
        return in.readFloat();
    }

    public double readDouble() throws IOException {
        bitSet = null;
        return in.readDouble();
    }

    public String readLine() throws IOException {
        bitSet = null;
        return in.readLine();
    }

    public String readUTF() throws IOException {
        bitSet = null;
        return in.readUTF();
    }
}
