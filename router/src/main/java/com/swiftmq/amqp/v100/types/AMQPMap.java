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

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A polymorphic mapping from distinct keys to values
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPMap extends AMQPType {
    Map<AMQPType, AMQPType> map = null;
    byte[] bytes;
    byte[] valueBytes = null;

    /**
     * Constructs an empty AMQPMap
     */
    public AMQPMap() {
        super("map", AMQPTypeDecoder.UNKNOWN);
    }

    /**
     * Constructs an AMQPMap with an initial value.
     *
     * @param map initial value
     * @throws IOException on encode error
     */
    public AMQPMap(Map<AMQPType, AMQPType> map) throws IOException {
        super("map", AMQPTypeDecoder.UNKNOWN);
        setValue(map);
    }

    /**
     * Sets the value
     *
     * @param map value
     * @throws IOException on encode error
     */
    public void setValue(Map<AMQPType, AMQPType> map) throws IOException {
        this.map = map;
        if (map != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                ((AMQPType) entry.getKey()).writeContent(dos);
                ((AMQPType) entry.getValue()).writeContent(dos);
            }
            dos.close();
            valueBytes = bos.toByteArray();
            bytes = null;
            if (valueBytes.length > 254 || map.size() * 2 > 255)
                code = AMQPTypeDecoder.MAP32;
            else
                code = AMQPTypeDecoder.MAP8;
        }
    }

    /**
     * Returns the value.
     *
     * @return value
     * @throws IOException on decode error
     */
    public Map<AMQPType, AMQPType> getValue() throws IOException {
        if (map == null) {
            map = new HashMap<AMQPType, AMQPType>();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            int n = 0;
            if (code == AMQPTypeDecoder.MAP8)
                n = dis.readByte() & 0xff;
            else if (code == AMQPTypeDecoder.MAP32)
                n = dis.readInt();
            else
                throw new IOException("Invalid code: " + code);
            if (n < 0)
                throw new IOException("Invalid map element count: " + n);
            for (int i = 0; i < n; i += 2)
                map.put(AMQPTypeDecoder.decode(dis), AMQPTypeDecoder.decode(dis));
        }
        return map;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (bytes != null)
            n += bytes.length;
        else {
            if (code == AMQPTypeDecoder.MAP8)
                n += 1;
            else
                n += 4;
            if (valueBytes != null)
                n += valueBytes.length;
        }
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        int len = 0;
        if (code == AMQPTypeDecoder.MAP8)
            len = in.readByte() & 0xff;
        else if (code == AMQPTypeDecoder.MAP32)
            len = in.readInt();
        else
            throw new IOException("Invalid code: " + code);
        if (len < 0)
            throw new IOException("byte[] array length invalid: " + len);
        bytes = new byte[len];
        in.readFully(bytes);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (bytes != null) {
            if (code == AMQPTypeDecoder.MAP8)
                out.writeByte(bytes.length);
            else if (code == AMQPTypeDecoder.MAP32)
                out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            if (code == AMQPTypeDecoder.MAP8) {
                out.writeByte(valueBytes.length + 1);
                out.writeByte(map.size() * 2);
            } else if (code == AMQPTypeDecoder.MAP32) {
                out.writeInt(valueBytes.length + 4);
                out.writeInt(map.size() * 2);
            }
            out.write(valueBytes);
        }
    }

    public String getValueString() {
        Map m = null;
        try {
            m = getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean _first = true;
        StringBuffer b = new StringBuffer("[");
        for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); ) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            Map.Entry entry = (Map.Entry) iter.next();
            b.append(((AMQPType) entry.getKey()).getValueString());
            b.append("=");
            b.append(((AMQPType) entry.getValue()).getValueString());
        }
        b.append("]");
        return b.toString();
    }

    public String toString() {
        return "[AMQPMap, map=" + map + ", bytes.length=" + (bytes != null ? bytes.length : "null") + ", valueBytes.length=" + (valueBytes != null ? valueBytes.length : "null") + super.toString() + "]";
    }
}