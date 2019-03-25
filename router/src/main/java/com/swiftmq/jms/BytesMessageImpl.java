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

import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

/**
 * Implementation of a BytesMessage.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class BytesMessageImpl extends MessageImpl implements BytesMessage {
    static final int WRITE_ONLY = 0;
    static final int READ_ONLY = 1;
    int mode = WRITE_ONLY;

    DataByteArrayOutputStream dos = null;
    DataByteArrayInputStream dis = null;
    byte[] array = null;
    int cnt = 0;

    /**
     * Creates a new BytesMessageImpl.
     * The message is set to write-only mode.
     */
    public BytesMessageImpl() {
        dos = null;
        dis = null;
        array = null;
        cnt = 0;
        mode = WRITE_ONLY;
    }

    public BytesMessageImpl(byte[] array, int cnt) {
        super();
        this.array = array;
        this.cnt = cnt;
    }

    protected int getType() {
        return TYPE_BYTESMESSAGE;
    }

    protected void writeBody(DataOutput out) throws IOException {
        if (dos != null) {
            array = dos.getBuffer();
            cnt = dos.getCount();
        }
        out.writeInt(cnt);
        if (cnt > 0)
            out.write(array, 0, cnt);
    }

    protected void readBody(DataInput in) throws IOException {
        mode = READ_ONLY;
        cnt = in.readInt();
        if (cnt > 0) {
            array = new byte[cnt];
            in.readFully(array);
        } else
            array = null;
    }

    private void checkRead() {
        if (dis == null) {
            dos = null;
            byte[] b = null;
            if (array != null && cnt > 0) {
                b = array;
            } else
                b = new byte[0];
            dis = new DataByteArrayInputStream(b);
        }
    }

    private void checkWrite() {
        if (dos == null) {
            dos = new DataByteArrayOutputStream();
            cnt = 0;
            array = null;
            dis = null;
        }
    }

    // For internal use to ensure JMS 1.0.2 compatibility
    public long _getBodyLength() {
        if (cnt == 0) {
            if (mode == WRITE_ONLY) {
                checkWrite();
                return dos.getCount();
            } else {
                checkRead();
                return dis.getMax();
            }
        } else
            return cnt;
    }

    public byte[] _getBody() {
        return array;
    }

    // JMS 1.1
    public long getBodyLength() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        return _getBodyLength();
    }

    public boolean readBoolean() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        boolean b;
        try {
            b = dis.readBoolean();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a signed 8-bit value from the stream message.
     *
     * @return the next byte from the stream message as a signed 8-bit
     * <code>byte</code>.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public byte readByte() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        byte b;
        try {
            b = dis.readByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read an unsigned 8-bit number from the stream message.
     *
     * @return the next byte from the stream message, interpreted as an
     * unsigned 8-bit number.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public int readUnsignedByte() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        int b;
        try {
            b = dis.readUnsignedByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a signed 16-bit number from the stream message.
     *
     * @return the next two bytes from the stream message, interpreted as a
     * signed 16-bit number.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public short readShort() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        short b;
        try {
            b = dis.readShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read an unsigned 16-bit number from the stream message.
     *
     * @return the next two bytes from the stream message, interpreted as an
     * unsigned 16-bit integer.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public int readUnsignedShort() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        int b;
        try {
            b = dis.readUnsignedShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a Unicode character value from the stream message.
     *
     * @return the next two bytes from the stream message as a Unicode
     * character.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public char readChar() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        char b;
        try {
            b = dis.readChar();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a signed 32-bit integer from the stream message.
     *
     * @return the next four bytes from the stream message, interpreted as
     * an <code>int</code>.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public int readInt() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();

        int b;
        try {
            b = dis.readInt();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a signed 64-bit integer from the stream message.
     *
     * @return the next eight bytes from the stream message, interpreted as
     * a <code>long</code>.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public long readLong() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        long b;
        try {
            b = dis.readLong();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a <code>float</code> from the stream message.
     *
     * @return the next four bytes from the stream message, interpreted as
     * a <code>float</code>.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public float readFloat() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        float b;
        try {
            b = dis.readFloat();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a <code>double</code> from the stream message.
     *
     * @return the next eight bytes from the stream message, interpreted as
     * a <code>double</code>.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public double readDouble() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();

        double b;
        try {
            b = dis.readDouble();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read in a string that has been encoded using a modified UTF-8
     * format from the stream message.
     * <p/>
     * <P>For more information on the UTF-8 format, see "File System Safe
     * UCS Transformation Format (FSS_UFT)", X/Open Preliminary Specification,
     * X/Open Company Ltd., Document Number: P316. This information also
     * appears in ISO/IEC 10646, Annex P.
     *
     * @return a Unicode string from the stream message.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public String readUTF() throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();

        String b;
        try {
            b = dis.readUTF();
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return b;
    }

    /**
     * Read a byte array from the stream message.
     *
     * @param value the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if
     * there is no more data because the end of the stream has been reached.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public int readBytes(byte[] value) throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();

        int b;
        try {
            b = dis.read(value);
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return (b);
    }

    /**
     * Read a portion of the bytes message.
     *
     * @param value  the buffer into which the data is read.
     * @param length the number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if
     * there is no more data because the end of the stream has been reached.
     * @throws MessageNotReadableException if message in write-only mode.
     * @throws MessageEOFException         if end of message stream
     * @throws JMSException                if JMS fails to read message due to
     *                                     some internal JMS error.
     */
    public int readBytes(byte[] value, int length) throws JMSException {
        if (mode == WRITE_ONLY) {
            throw new MessageNotReadableException("message is in write-only mode");
        }
        checkRead();
        int b;
        try {
            b = dis.read(value, 0, length);
        } catch (EOFException e) {
            throw new MessageEOFException(e.toString());
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }

        return (b);
    }

    /**
     * Write a <code>boolean</code> to the stream message as a 1-byte value.
     * The value <code>true</code> is written out as the value
     * <code>(byte)1</code>; the value <code>false</code> is written out as
     * the value <code>(byte)0</code>.
     *
     * @param value the <code>boolean</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeBoolean(boolean value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeBoolean(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write out a <code>byte</code> to the stream message as a 1-byte value.
     *
     * @param value the <code>byte</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeByte(byte value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeByte(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a <code>short</code> to the stream message as two bytes, high
     * byte first.
     *
     * @param value the <code>short</code> to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeShort(short value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }

        checkWrite();
        try {
            dos.writeShort(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a <code>char</code> to the stream message as a 2-byte value,
     * high byte first.
     *
     * @param value the <code>char</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeChar(char value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeChar(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write an <code>int</code> to the stream message as four bytes,
     * high byte first.
     *
     * @param value the <code>int</code> to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeInt(int value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeInt(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a <code>long</code> to the stream message as eight bytes,
     * high byte first.
     *
     * @param value the <code>long</code> to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeLong(long value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeLong(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Convert the float argument to an <code>int</code> using the
     * <code>floatToIntBits</code> method in class <code>Float</code>,
     * and then writes that <code>int</code> value to the stream
     * message as a 4-byte quantity, high byte first.
     *
     * @param value the <code>float</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeFloat(float value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeFloat(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Convert the double argument to a <code>long</code> using the
     * <code>doubleToLongBits</code> method in class <code>Double</code>,
     * and then writes that <code>long</code> value to the stream
     * message as an 8-byte quantity, high byte first.
     *
     * @param value the <code>double</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeDouble(double value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeDouble(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a string to the stream message using UTF-8 encoding in a
     * machine-independent manner.
     * <p/>
     * <P>For more information on the UTF-8 format, see "File System Safe
     * UCS Transformation Format (FSS_UFT)", X/Open Preliminary Specification,
     * X/Open Company Ltd., Document Number: P316. This information also
     * appears in ISO/IEC 10646, Annex P.
     *
     * @param value the <code>String</code> value to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeUTF(String value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        checkWrite();
        try {
            dos.writeUTF(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a byte array to the stream message.
     *
     * @param value the byte array to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */

    public void writeBytes(byte[] value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }

        checkWrite();
        try {
            dos.write(value);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a portion of a byte array to the stream message.
     *
     * @param value  the byte array value to be written.
     * @param offset the initial offset within the byte array.
     * @param length the number of bytes to use.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeBytes(byte[] value, int offset,
                           int length) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }

        checkWrite();
        try {
            dos.write(value, offset, length);
        } catch (IOException e) {
            throw new JMSException(e.toString());
        }
    }

    /**
     * Write a Java object to the stream message.
     * <p/>
     * <P>Note that this method only works for the objectified primitive
     * object types (Integer, Double, Long ...), String's and byte arrays.
     *
     * @param value the Java object to be written.
     * @throws MessageNotWriteableException if message in read-only mode.
     * @throws MessageFormatException       if object is invalid type.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     */
    public void writeObject(Object value) throws JMSException {
        if (mode == READ_ONLY) {
            throw new MessageNotWriteableException("message is in read-only mode");
        }
        if (value == null)
            throw new NullPointerException("null values are not support by the JMS spec");

        checkWrite();
        if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else if (value instanceof String) {
            writeUTF((String) value);
        } else {
            throw new MessageFormatException("writeObject supports only Boolean, Byte, Short, Integer, Long, Float, Double, byte[], or String");
        }
    }

    /**
     * Put the message in read-only mode, and reposition the stream of
     * bytes to the beginning.
     *
     * @throws JMSException           if JMS fails to reset the message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if message has an invalid
     *                                format
     */
    public void reset() throws JMSException {
        mode = READ_ONLY;
        if (dis != null) {
            try {
                dis.reset();
            } catch (IOException e) {
                throw new JMSException(e.toString());
            }
        } else {
            if (dos != null) {
                dis = new DataByteArrayInputStream(dos);
            } else {
                dis = null;
            }
        }
    }

    /**
     * Clear out the message body. All other parts of the message are left
     * untouched.
     *
     * @throws JMSException if JMS fails to due to some internal JMS error.
     */
    public void clearBody() throws JMSException {
        mode = WRITE_ONLY;
        dis = null;
        dos = null;
        array = null;
        cnt = 0;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[BytesMessageImpl ");
        b.append(super.toString());
        b.append(" dis=");
        b.append(dis);
        b.append(" dos=");
        b.append(dos);
        b.append(" array=");
        b.append(array);
        b.append("]");
        return b.toString();
    }
}



