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

package com.swiftmq.jms;

import com.swiftmq.jms.primitives.*;
import com.swiftmq.tools.dump.Dumpable;

import javax.jms.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a StreamMessage.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class StreamMessageImpl extends MessageImpl implements StreamMessage {
    static final int WRITE_ONLY = 0;
    static final int READ_ONLY = 1;
    static final int DEFAULT_SIZE = 32;
    int mode = WRITE_ONLY;
    int pos = 0;
    byte[] lastBA = null;
    int amount = 0;
    Primitive[] elements = null;
    int nElements = 0;

    protected int getType() {
        return TYPE_STREAMMESSAGE;
    }

    protected void writeBody(DataOutput out) throws IOException {
        out.writeInt(nElements);
        if (nElements > 0) {
            for (int i = 0; i < nElements; i++) {
                Dumpable d = (Dumpable) elements[i];
                out.writeInt(d.getDumpId());
                d.writeContent(out);
            }
        }
    }

    protected void readBody(DataInput in) throws IOException {
        nElements = in.readInt();
        if (nElements > 0) {
            elements = new Primitive[nElements];
            for (int i = 0; i < elements.length; i++) {
                Dumpable d = (Dumpable) PrimitiveFactory.createInstance(in.readInt());
                d.readContent(in);
                elements[i] = (Primitive) d;
            }
        }
    }

    /**
     * Return a given value at the given index; throws an JMSException if not defined.
     *
     * @param index the index of the value
     * @return the value with the given index.
     * @throws MessageEOFException if index >= vector.size
     */
    private Object getValue(int index) throws MessageEOFException {
        if (elements == null || index >= nElements) {
            throw new MessageEOFException("end-of-stream reached");
        }

        Primitive primitive = elements[index];
        Object obj = primitive.getObject();
        if (!(obj instanceof byte[])) {
            lastBA = null;
            amount = 0;
        }
        return obj;
    }

    /**
     * Read a <code>boolean</code> from the stream message.
     *
     * @return the <code>boolean</code> value read.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public boolean readBoolean() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Boolean) {
            return ((Boolean) obj).booleanValue();
        } else if (obj instanceof String) {
            try {
                return (Boolean.valueOf((String) obj)).booleanValue();
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + (String) obj
                        + " to boolean");
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to boolean");
    }

    /**
     * Read a byte value from the stream message.
     *
     * @return the next byte from the stream message as a 8-bit
     * <code>byte</code>.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public byte readByte() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Byte) {
            return ((Byte) obj).byteValue();
        } else if (obj instanceof String) {
            try {
                return (new Byte((String) obj).byteValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to byte");
    }

    /**
     * Read a 16-bit number from the stream message.
     *
     * @return a 16-bit number from the stream message.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public short readShort() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Short) {
            return ((Short) obj).shortValue();
        } else if (obj instanceof String) {
            try {
                return (new Short((String) obj).shortValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        } else if (obj instanceof Byte) {
            try {
                return (((Byte) obj).shortValue());
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + (Long) obj
                        + " to short");
            }
        }
        pos--;
        throw new MessageFormatException("can't convert property value to short");
    }

    /**
     * Read a Unicode character value from the stream message.
     *
     * @return a Unicode character from the stream message.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */

    public char readChar() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);
        if (obj == null) {
            pos--;
            throw new NullPointerException();
        }

        if (obj instanceof Character) {
            return ((Character) obj).charValue();
        }
        pos--;
        throw new MessageFormatException("can't convert message value to char");
    }

    /**
     * Read a 32-bit integer from the stream message.
     *
     * @return a 32-bit integer value from the stream message, interpreted
     * as a <code>int</code>.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public int readInt() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Integer) {
            return ((Integer) obj).intValue();
        } else if (obj instanceof String) {
            try {
                return (Integer.valueOf((String) obj).intValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        } else if (obj instanceof Short) {
            try {
                return (((Short) obj).intValue());
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to int");
            }
        } else if (obj instanceof Byte) {
            try {
                return (((Byte) obj).intValue());
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to int");
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to int");
    }

    /**
     * Read a 64-bit integer from the stream message.
     *
     * @return a 64-bit integer value from the stream message, interpreted as
     * a <code>long</code>.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public long readLong() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Long) {
            return ((Long) obj).longValue();
        } else if (obj instanceof String) {
            try {
                return (Long.valueOf((String) obj).longValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        } else if (obj instanceof Short) {
            try {
                return ((Short) obj).longValue();
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to long");
            }
        } else if (obj instanceof Integer) {
            try {
                return ((Integer) obj).longValue();
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to long");
            }
        } else if (obj instanceof Byte) {
            try {
                return ((Byte) obj).longValue();
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to long");
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to long");
    }

    /**
     * Read a <code>float</code> from the stream message.
     *
     * @return a <code>float</code> value from the stream message.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public float readFloat() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Float) {
            return ((Float) obj).floatValue();
        } else if (obj instanceof String) {
            try {
                return (new Float((String) obj).floatValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to float");
    }

    /**
     * Read a <code>double</code> from the stream message.
     *
     * @return a <code>double</code> value from the stream message.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public double readDouble() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj instanceof Double) {
            return ((Double) obj).doubleValue();
        } else if (obj instanceof Float) {
            try {
                return (((Float) obj).doubleValue());
            } catch (Exception e) {
                pos--;
                throw new MessageFormatException("can't convert " + obj
                        + " to double");
            }
        } else if (obj instanceof String) {
            try {
                return (new Double((String) obj).doubleValue());
            } catch (NumberFormatException e) {
                pos--;
                throw e;
            }
        }
        pos--;
        throw new MessageFormatException("can't convert message value to double");
    }

    /**
     * Read in a string from the stream message.
     *
     * @return a Unicode string from the stream message.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public String readString() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object obj = getValue(pos++);

        if (obj == null) {
            return null;
        }

        if (obj instanceof byte[])
            throw new MessageFormatException("Type conversion from byte[] to String is invalid");

        if (obj instanceof String) {
            return ((String) obj);
        }

        return obj.toString();
    }

    /**
     * Read a byte array from the stream message.
     *
     * @param value the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if
     * there is no more data because the end of the stream has been reached.
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageFormatException      if this type conversion is invalid
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public int readBytes(byte[] value) throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        if (lastBA != null) {
            if (amount == lastBA.length) {
                lastBA = null;
                amount = 0;
                return -1;
            } else {
                int rest = lastBA.length - amount;
                int toRead = Math.min(rest, value.length);
                System.arraycopy(lastBA, amount, value, 0, toRead);
                amount += toRead;
                return toRead;
            }
        } else {
            Object obj = getValue(pos++);

            if (obj == null) {
                return -1;
            }

            if (obj instanceof byte[]) {
                lastBA = (byte[]) obj;
                int len = Math.min(lastBA.length, value.length);
                System.arraycopy(lastBA, 0, value, 0, len);
                amount = len;
                return len;
            }
            pos--;
            throw new MessageFormatException("can't convert message value to byte[]");
        }
    }

    /**
     * Read a Java object from the stream message.
     * <p/>
     * <P>Note that this method can be used to return in objectified format,
     * an object that had been written to the Stream with the equivalent
     * <CODE>writeObject</CODE> method call, or it's equivalent primitive
     * write<type> method.
     *
     * @return a Java object from the stream message, in objectified
     * format (ie. if it set as an int, then a Integer is returned).
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     * @throws MessageEOFException         if an end of message stream
     * @throws MessageNotReadableException if message in write-only mode.
     */
    public Object readObject() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }

        Object value = getValue(pos++);
        Object newValue = null;
        if (value instanceof Character)
            newValue = new Character(((Character) value).charValue());
        else if (value instanceof Boolean)
            newValue = new Boolean(((Boolean) value).booleanValue());
        else if (value instanceof Byte)
            newValue = new Byte(((Byte) value).byteValue());
        else if (value instanceof Short)
            newValue = new Short(((Short) value).shortValue());
        else if (value instanceof Integer)
            newValue = new Integer(((Integer) value).intValue());
        else if (value instanceof Long)
            newValue = new Long(((Long) value).longValue());
        else if (value instanceof Float)
            newValue = new Float(((Float) value).floatValue());
        else if (value instanceof Double)
            newValue = new Double(((Double) value).doubleValue());
        else if (value instanceof String)
            newValue = new String((String) value);
        else if (value instanceof byte[])
            newValue = ((byte[]) value).clone();
        return newValue;
    }

    private void checkElements() {
        if (elements == null) {
            elements = new Primitive[DEFAULT_SIZE];
            nElements = 0;
        } else {
            if (nElements == elements.length) {
                Primitive[] p = new Primitive[elements.length + DEFAULT_SIZE];
                System.arraycopy(elements, 0, p, 0, nElements);
                elements = p;
            }
        }
    }

    /**
     * Write a <code>boolean</code> to the stream message.
     * The value <code>true</code> is written out as the value
     * <code>(byte)1</code>; the value <code>false</code> is written out as
     * the value <code>(byte)0</code>.
     *
     * @param value the <code>boolean</code> value to be written.
     * @throws JMSException                 if JMS fails to read message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeBoolean(boolean value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Boolean(value);
    }

    /**
     * Write out a <code>byte</code> to the stream message.
     *
     * @param value the <code>byte</code> value to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeByte(byte value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Byte(value);
    }

    /**
     * Write a <code>short</code> to the stream message.
     *
     * @param value the <code>short</code> to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeShort(short value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Short(value);
    }

    /**
     * Write a <code>char</code> to the stream message.
     *
     * @param value the <code>char</code> value to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeChar(char value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Char(value);
    }

    /**
     * Write an <code>int</code> to the stream message.
     *
     * @param value the <code>int</code> to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeInt(int value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Int(value);
    }

    /**
     * Write a <code>long</code> to the stream message.
     *
     * @param value the <code>long</code> to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeLong(long value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Long(value);
    }

    /**
     * Write a <code>float</code> to the stream message.
     *
     * @param value the <code>float</code> value to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeFloat(float value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Float(value);
    }

    /**
     * Write a <code>double</code> to the stream message.
     *
     * @param value the <code>double</code> value to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeDouble(double value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _Double(value);
    }

    /**
     * Write a string to the stream message.
     *
     * @param value the <code>String</code> value to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeString(String value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkElements();
        elements[nElements++] = new _String(value);
    }

    /**
     * Write a byte array to the stream message.
     *
     * @param value the byte array to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeBytes(byte[] value) throws JMSException {
        writeBytes(value, 0, value.length);
    }

    /**
     * Write a portion of a byte array to the stream message.
     *
     * @param value  the byte array value to be written.
     * @param offset the initial offset within the byte array.
     * @param length the number of bytes to use.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void writeBytes(byte[] value, int offset,
                           int length) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }

        byte[] array = new byte[length];

        System.arraycopy(value, offset, array, 0, length);
        checkElements();
        elements[nElements++] = new _Bytes(array);
    }

    /**
     * Write a Java object to the stream message.
     * <p/>
     * <P>Note that this method only works for the objectified primitive
     * object types (Integer, Double, Long ...), String's and byte arrays.
     *
     * @param value the Java object to be written.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws MessageFormatException       if the object is invalid
     */
    public void writeObject(Object value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }

        Primitive newValue = null;
        if (value == null)
            newValue = new _String(null);
        else if (value instanceof Character)
            newValue = new _Char(((Character) value).charValue());
        else if (value instanceof Boolean)
            newValue = new _Boolean(((Boolean) value).booleanValue());
        else if (value instanceof Byte)
            newValue = new _Byte(((Byte) value).byteValue());
        else if (value instanceof Short)
            newValue = new _Short(((Short) value).shortValue());
        else if (value instanceof Integer)
            newValue = new _Int(((Integer) value).intValue());
        else if (value instanceof Long)
            newValue = new _Long(((Long) value).longValue());
        else if (value instanceof Float)
            newValue = new _Float(((Float) value).floatValue());
        else if (value instanceof Double)
            newValue = new _Double(((Double) value).doubleValue());
        else if (value instanceof String)
            newValue = new _String((String) value);
        else if (value instanceof byte[])
            newValue = new _Bytes((byte[]) value);

        if (newValue != null) {
            checkElements();
            elements[nElements++] = newValue;
        } else {
            throw new JMSException("writeObject supports only Boolean, Byte, Short, Integer, Long, Float, Double, byte[], or String");
        }
    }

    /**
     * Put the message in read-only mode, and reposition the stream
     * to the beginning.
     *
     * @throws JMSException           if JMS fails to reset the message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if message has an invalid
     *                                format
     */
    public void reset() throws JMSException {
        pos = 0;
        mode = READ_ONLY;
    }

    /**
     * Clear out the message body. All other parts of the message are left
     * untouched.
     *
     * @throws JMSException if JMS fails to due to some internal JMS error.
     */
    public void clearBody() throws JMSException {
        elements = null;
        nElements = 0;
        pos = 0;
        mode = WRITE_ONLY;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[StreamMessageImpl ");
        b.append(super.toString());
        b.append(" elements=");
        b.append(elements);
        b.append(" nElements=");
        b.append(nElements);
        b.append("]");
        return b.toString();
    }
}



