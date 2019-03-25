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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class Field {
    int type = 'V';
    Object value = null;
    String svalue = null;

    public Field(int type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Field() {
    }

    public int getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getStringValue() {
        return svalue;
    }

    public Field readContent(DataInput in) throws IOException {
        type = in.readUnsignedByte();
        switch (type) {
            case 't':
                value = new Boolean(in.readBoolean());
                break;
            case 'b':
                value = new Integer(in.readByte());
                break;
            case 'B':
                value = new Integer(in.readUnsignedByte());
                break;
            case 'U':
                value = new Integer(Coder.readShort(in));
                break;
            case 'u':
                value = new Integer(Coder.readUnsignedShort(in));
                break;
            case 'I':
                value = new Integer(in.readInt());
                break;
            case 'i':
                value = new Long(Coder.readUnsignedInt(in));
                break;
            case 'L':
                value = new Long(Coder.readLong(in));
                break;
            case 'l':
                byte b[] = new byte[8];
                in.readFully(b);
                value = b;
                break;
            case 'f':
                value = new Float(Coder.readFloat(in));
                break;
            case 'd':
                value = new Double(Coder.readDouble(in));
                break;
            case 'D':
                byte d[] = new byte[5];
                in.readFully(d);
                value = d;
                break;
            case 's':
                value = Coder.readShort(in);
                break;
            case 'S':
                value = Coder.readLongString(in);
                Charset charset = Charset.forName("UTF-8");
                svalue = charset.decode(ByteBuffer.wrap((byte[]) value)).toString();
                break;
            case 'A':
                value = Coder.readArray(in);
                break;
            case 'T':
                value = Coder.readLong(in);
                break;
            case 'F':
                value = Coder.readTable(in);
                break;
            case 'V':
                break;
        }
        return this;
    }

    public void writeContent(DataOutput out) throws IOException {
        out.writeByte(type);
        switch (type) {
            case 't':
                Coder.writeByte(((Boolean) value).booleanValue() ? 1 : 0, out);
                break;
            case 'b':
                Coder.writeByte(((Integer) value).byteValue(), out);
                break;
            case 'B':
                Coder.writeByte(((Integer) value).intValue(), out);
                break;
            case 'U':
                Coder.writeShort(((Integer) value).shortValue(), out);
                break;
            case 'u':
                Coder.writeShort(((Integer) value).intValue(), out);
                break;
            case 'I':
                Coder.writeInt(((Integer) value).intValue(), out);
                break;
            case 'i':
                Coder.writeInt(((Long) value).intValue(), out);
                break;
            case 'L':
                Coder.writeLong(((Long) value).longValue(), out);
                break;
            case 'l':
                Coder.writeBytes((byte[]) value, out);
                break;
            case 'f':
                Coder.writeFloat(((Float) value).floatValue(), out);
                break;
            case 'd':
                Coder.writeDouble(((Double) value).doubleValue(), out);
                break;
            case 'D':
                Coder.writeBytes((byte[]) value, out);
                break;
            case 's':
                Coder.writeShort((Short) value, out);
                break;
            case 'S':
                Coder.writeLongString((byte[]) value, out);
                break;
            case 'A':
                Coder.writeArray((List) value, out);
                break;
            case 'T':
                Coder.writeLong(((Long) value).longValue(), out);
                break;
            case 'F':
                Coder.writeTable((Map) value, out);
                break;
            case 'V':
                break;
        }
    }

    public String toString() {
        return "[Field type=" + (char) type + ", value=" + (svalue != null ? svalue : value) + "]";
    }
}
