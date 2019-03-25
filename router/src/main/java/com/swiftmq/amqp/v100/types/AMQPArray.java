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

/**
 * A sequence of values of a single type
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPArray extends AMQPType {
    int typeCode = -1;
    AMQPDescribedConstructor eleConstructor = null;
    AMQPType[] array = null;
    byte[] bytes = null;
    byte[] valueBytes = null;

    /**
     * Constructs an empty AMQPArray of unknown element type.
     */
    public AMQPArray() {
        super("array", AMQPTypeDecoder.UNKNOWN);
    }

    /**
     * Constructs an AMQPArray with a element constructor.
     *
     * @param eleConstructor element constructor
     * @param array          the actual array
     * @throws IOException on encode error
     */
    public AMQPArray(AMQPDescribedConstructor eleConstructor, AMQPType[] array) throws IOException {
        super("array", AMQPTypeDecoder.UNKNOWN);
        setValue(eleConstructor, array);
    }

    /**
     * Constructs an AMQPArray with a element type code.
     *
     * @param typeCode element type code
     * @param array    the actual array
     * @throws IOException on encode error
     */
    public AMQPArray(int typeCode, AMQPType[] array) throws IOException {
        super("array", AMQPTypeDecoder.UNKNOWN);
        setValue(typeCode, array);
    }

    private void writeNoCode(DataOutputStream dos, AMQPType t) throws IOException {
        boolean oldValue = t.isWriteCode();
        t.setWriteCode(false);
        t.writeContent(dos);
        t.setWriteCode(oldValue);
    }

    /**
     * Sets the value with a element constructor.
     *
     * @param eleConstructor element constructor
     * @param array          the actual array
     * @throws IOException on encode error
     */
    public void setValue(AMQPDescribedConstructor eleConstructor, AMQPType[] array) throws IOException {
        this.eleConstructor = eleConstructor;
        this.array = array;
        if (array != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            eleConstructor.writeContent(dos);
            for (int i = 0; i < array.length; i++)
                writeNoCode(dos, array[i]);
            dos.close();
            valueBytes = bos.toByteArray();
            bytes = null;
            if (valueBytes.length > 255 || array.length > 255)
                code = AMQPTypeDecoder.ARRAY32;
            else
                code = AMQPTypeDecoder.ARRAY8;
        }
    }

    /**
     * Sets the value with a element type code.
     *
     * @param typeCode element type code
     * @param array    the actual array
     * @throws IOException on encode error
     */
    public void setValue(int typeCode, AMQPType[] array) throws IOException {
        this.typeCode = typeCode;
        this.array = array;
        if (array != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeByte(typeCode);
            for (int i = 0; i < array.length; i++)
                writeNoCode(dos, array[i]);
            dos.close();
            valueBytes = bos.toByteArray();
            bytes = null;
            if (valueBytes.length > 254 || array.length > 255)
                code = AMQPTypeDecoder.ARRAY32;
            else
                code = AMQPTypeDecoder.ARRAY8;
        }
    }

    /**
     * Returns the value.
     *
     * @return value
     * @throws IOException on decode error
     */
    public AMQPType[] getValue() throws IOException {
        if (array == null) {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            int n = 0;
            if (code == AMQPTypeDecoder.ARRAY8)
                n = dis.readUnsignedByte();
            else if (code == AMQPTypeDecoder.ARRAY32)
                n = dis.readInt();
            else
                throw new IOException("Invalid code: " + code);
            if (n < 0)
                throw new IOException("Invalid array element count: " + n);
            typeCode = dis.readUnsignedByte();
            if (typeCode == AMQPTypeDecoder.CONSTRUCTOR) {
                eleConstructor = new AMQPDescribedConstructor();
                eleConstructor.readContent(dis);
                typeCode = eleConstructor.getFormatCode();
            }
            array = new AMQPType[n];
            for (int i = 0; i < n; i++) {
                array[i] = AMQPTypeDecoder.decode(typeCode, dis);
            }
        }
        return array;
    }

    /**
     * Returns the element constructor
     *
     * @return element constructor
     * @throws IOException on decode error
     */
    public AMQPDescribedConstructor getEleConstructor() throws IOException {
        getValue();
        return eleConstructor;
    }

    /**
     * Returns the element type code
     *
     * @return type code
     * @throws IOException on decode error
     */
    public int getTypeCode() throws IOException {
        getValue();
        return typeCode;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (eleConstructor != null)
            n += eleConstructor.getPredictedSize();
        if (code == AMQPTypeDecoder.ARRAY8)
            n += 1;
        else
            n += 4;
        if (bytes != null)
            n += bytes.length;
        else {
            if (array != null) {
                for (int i = 0; i < array.length; i++)
                    n += array[i].getPredictedSize();
            }
        }
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        int len = 0;
        if (code == AMQPTypeDecoder.ARRAY8)
            len = in.readUnsignedByte();
        else if (code == AMQPTypeDecoder.ARRAY32)
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
            if (code == AMQPTypeDecoder.ARRAY8)
                out.writeByte(bytes.length);
            else if (code == AMQPTypeDecoder.ARRAY32)
                out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            if (code == AMQPTypeDecoder.ARRAY8) {
                out.writeByte(valueBytes.length + 1);
                out.writeByte(array.length);
            } else if (code == AMQPTypeDecoder.ARRAY32) {
                out.writeInt(valueBytes.length + 4);
                out.writeInt(array.length);
            }
            out.write(valueBytes);
        }
    }

    private String printArray() {
        StringBuffer b = new StringBuffer("\n");
        for (int i = 0; i < array.length; i++) {
            b.append(i);
            b.append(": ");
            b.append(array[i].toString());
            b.append("\n");
        }
        return b.toString();
    }

    public String getValueString() {
        try {
            getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[");
        for (int i = 0; i < array.length; i++) {
            if (i > 0)
                b.append(", ");
            b.append(array[i].getValueString());
        }
        b.append("]");
        return b.toString();
    }

    public String toString() {
        try {
            getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "[AMQPArray, eleConstructor=" + eleConstructor + ", typeCode=" + typeCode + ", array=" + (array == null ? "null" : printArray()) + ", bytes.length=" + (bytes != null ? bytes.length : "null") + ", valueBytes.length=" + (valueBytes != null ? valueBytes.length : "null") + super.toString() + "]";
    }
}