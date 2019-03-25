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

package com.swiftmq.amqp.v100.types;

import com.swiftmq.amqp.ProtocolHeader;

import java.util.List;

public class Util {
    public static final ProtocolHeader SASL_INIT = new ProtocolHeader("AMQP", 3, 1, 0, 0);
    public static final ProtocolHeader AMQP_INIT = new ProtocolHeader("AMQP", 0, 1, 0, 0);

    public static void ensureSize(List list, int size) {
        if (list.size() < size + 1) {
            for (int i = list.size(); i < size + 1; i++)
                list.add(null);
        }
    }

    public static List addToList(List list, Object[] obj) {
        for (int i = 0; i < obj.length; i++)
            list.add(obj[i]);
        return list;
    }

    public static String toHex(byte[] bytes, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(String.format("0x%1$02X ", bytes[i]));
        }

        return sb.toString();
    }

    public static short readShort(byte[] b, int offset) {
        int pos = offset;
        int i1 = b[pos++] & 0xff;
        int i2 = b[pos++] & 0xff;
        return (short) ((i1 << 8) + (i2 << 0));
    }

    public static int readUnsignedShort(byte[] b, int offset) {
        int pos = offset;
        int i1 = b[pos++] & 0xff;
        int i2 = b[pos++] & 0xff;
        return ((i1 << 8) + (i2 << 0));
    }

    public static int readInt(byte[] b, int offset) {
        int pos = offset;
        int i1 = b[pos++] & 0xff;
        int i2 = b[pos++] & 0xff;
        int i3 = b[pos++] & 0xff;
        int i4 = b[pos++] & 0xff;
        int i = (i1 << 24) + (i2 << 16) + (i3 << 8) + (i4 << 0);
        return i;
    }

    public static long readLong(byte[] b, int offset) {
        long l = ((long) (readInt(b, offset)) << 32) + (readInt(b, offset + 4) & 0xFFFFFFFFL);
        return l;
    }

    public static float readFloat(byte[] b, int offset) {
        return Float.intBitsToFloat(readInt(b, offset));
    }

    public static double readDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(readLong(b, offset));
    }

    public static void writeShort(int s, byte[] b, int offset) {
        int pos = offset;
        b[pos++] = (byte) ((s >>> 8) & 0xFF);
        b[pos++] = (byte) ((s >>> 0) & 0xFF);
    }

    public static void writeInt(int i, byte[] b, int offset) {
        int pos = offset;
        b[pos++] = (byte) ((i >>> 24) & 0xFF);
        b[pos++] = (byte) ((i >>> 16) & 0xFF);
        b[pos++] = (byte) ((i >>> 8) & 0xFF);
        b[pos++] = (byte) ((i >>> 0) & 0xFF);
    }

    public static void writeLong(long l, byte[] b, int offset) {
        int pos = offset;
        b[pos++] = (byte) ((l >>> 56) & 0xFF);
        b[pos++] = (byte) ((l >>> 48) & 0xFF);
        b[pos++] = (byte) ((l >>> 40) & 0xFF);
        b[pos++] = (byte) ((l >>> 32) & 0xFF);
        b[pos++] = (byte) ((l >>> 24) & 0xFF);
        b[pos++] = (byte) ((l >>> 16) & 0xFF);
        b[pos++] = (byte) ((l >>> 8) & 0xFF);
        b[pos++] = (byte) ((l >>> 0) & 0xFF);
    }

    public static void writeFloat(float v, byte[] b, int offset) {
        writeInt(Float.floatToIntBits(v), b, offset);
    }

    public static void writeDouble(double v, byte[] b, int offset) {
        writeLong(Double.doubleToLongBits(v), b, offset);
    }
}
