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

package com.swiftmq.amqp.v091.types;

import com.swiftmq.amqp.v091.io.BitSupportDataInput;
import com.swiftmq.amqp.v091.io.BitSupportDataOutput;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

public class Coder {

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

    public static byte readByte(DataInput in) throws IOException {
        return in.readByte();
    }

    public static boolean readBit(BitSupportDataInput in) throws IOException {
        return in.readBit();
    }

    public static int readUnsignedByte(DataInput in) throws IOException {
        return readByte(in) & 0xff;
    }

    public static byte[] readBytes(DataInput in, int len) throws IOException {
        byte[] dest = new byte[len];
        in.readFully(dest);
        return dest;
    }

    public static short readShort(DataInput in) throws IOException {
        return in.readShort();
    }

    public static int readUnsignedShort(DataInput in) throws IOException {
        return in.readUnsignedShort();
    }

    public static int readInt(DataInput in) throws IOException {
        return in.readInt();
    }

    public static long readUnsignedInt(DataInput in) throws IOException {
        return readInt(in) & 0xffffffffL;
    }

    public static long readLong(DataInput in) throws IOException {
        return in.readLong();
    }

    public static String readShortString(DataInput in) throws IOException {
        int len = readUnsignedByte(in);
        Charset charset = Charset.forName("UTF-8");
        String s = charset.decode(ByteBuffer.wrap(readBytes(in, len))).toString();
        return s;
    }

    public static byte[] readLongString(DataInput in) throws IOException {
        int len = (int) readUnsignedInt(in);
        return readBytes(in, len);
    }

    public static float readFloat(DataInput in) throws IOException {
        return Float.intBitsToFloat(readInt(in));
    }

    public static double readDouble(DataInput in) throws IOException {
        return Double.longBitsToDouble(readLong(in));
    }

    private static Object readFieldValue(DataInput in) throws IOException {
        Field f = new Field().readContent(in);
        return f;
    }

    public static List readArray(DataInput in) throws IOException {
        List list = new ArrayList();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream(readLongString(in));
        while (dbis.available() > 0)
            list.add(readFieldValue(in));
        return list;
    }

    public static Map<String, Object> readTable(DataInput in) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        DataByteArrayInputStream dbis = new DataByteArrayInputStream(readLongString(in));
        while (dbis.available() > 0)
            map.put(readShortString(dbis), readFieldValue(dbis));
        return map;
    }

    public static void writeShort(int s, DataOutput out) throws IOException {
        out.writeShort(s);
    }

    public static void writeByte(int i, DataOutput out) throws IOException {
        out.writeByte(i);
    }

    public static void writeBit(boolean b, BitSupportDataOutput out) throws IOException {
        out.writeBit(b);
    }

    public static void writeBytes(byte[] src, int srcPos, int srcLen, DataOutput out) throws IOException {
        out.write(src, srcPos, srcLen);
    }

    public static void writeBytes(byte[] src, DataOutput out) throws IOException {
        writeBytes(src, 0, src.length, out);
    }

    public static void writeInt(int i, DataOutput out) throws IOException {
        out.writeInt(i);
    }

    public static void writeLong(long l, DataOutput out) throws IOException {
        out.writeLong(l);
    }

    public static void writeShortString(String s, DataOutput out) throws IOException {
        writeByte(s.length(), out);
        Charset charset = Charset.forName("UTF-8");
        ByteBuffer buffer = charset.encode(s);
        buffer.rewind();
        out.write(buffer.array(), 0, buffer.remaining());
    }

    public static void writeLongString(byte[] ls, DataOutput out) throws IOException {
        writeInt(ls.length, out);
        out.write(ls);
    }

    public static void writeFloat(float v, DataOutput out) throws IOException {
        writeInt(Float.floatToIntBits(v), out);
    }

    public static void writeDouble(double v, DataOutput out) throws IOException {
        writeLong(Double.doubleToLongBits(v), out);
    }

    public static void writeArray(List l, DataOutput out) throws IOException {
        if (l == null) {
            writeInt(0, out);
            return;
        }
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        for (int i = 0; i < l.size(); i++)
            ((Field) l.get(i)).writeContent(dbos);
    }

    public static void writeTable(Map<String, Object> m, DataOutput out) throws IOException {
        if (m == null || m.size() == 0) {
            writeInt(0, out);
            return;
        }
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            writeShortString((String) entry.getKey(), dbos);
            ((Field) entry.getValue()).writeContent(dbos);
        }
        writeInt(dbos.getCount(), out);
        writeBytes(dbos.getBuffer(), 0, dbos.getCount(), out);
    }

}
