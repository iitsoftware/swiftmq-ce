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

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;

/**
 * Implementation of a MapMessage.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class MapMessageImpl extends MessageImpl implements MapMessage {
    MessageProperties map = null;
    byte[] mapBytes = null;
    boolean bodyReadOnly = false;

    protected int getType() {
        return TYPE_MAPMESSAGE;
    }

    public void setReadOnly(boolean b) {
        super.setReadOnly(b);
        bodyReadOnly = b;
    }

    protected void writeBody(DataOutput out) throws IOException {
        if (map == null) {
            if (mapBytes == null)
                out.writeByte(0);
            else {
                out.writeByte(1);
                out.writeInt(mapBytes.length);
                out.write(mapBytes, 0, mapBytes.length);
            }
        } else {
            out.writeByte(1);
            DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
            map.writeContent(dos);
            dos.close();
            out.writeInt(dos.getCount());
            out.write(dos.getBuffer(), 0, dos.getCount());
        }
    }

    protected void readBody(DataInput in) throws IOException {
        byte set = in.readByte();
        if (set == 0) {
            map = null;
            mapBytes = null;
        } else {
            mapBytes = new byte[in.readInt()];
            in.readFully(mapBytes);
        }
    }

    protected void unfoldBody() {
        checkMap();
    }

    private void checkMap() {
        if (map == null)
            map = new MessageProperties();
        if (mapBytes != null) {
            try {
                DataByteArrayInputStream dis = new DataByteArrayInputStream(mapBytes);
                map.readContent(dis);
                mapBytes = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean getBoolean(String name) throws JMSException {
        checkMap();
        return map.getBoolean(name);
    }

    /**
     * Return the byte value with the given name.
     *
     * @param name the name of the byte
     * @return the byte value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public byte getByte(String name) throws JMSException {
        checkMap();
        return map.getByte(name);
    }

    /**
     * Return the short value with the given name.
     *
     * @param name the name of the short
     * @return the short value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public short getShort(String name) throws JMSException {
        checkMap();
        return map.getShort(name);
    }

    /**
     * Return the Unicode character value with the given name.
     *
     * @param name the name of the Unicode character
     * @return the Unicode character value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public char getChar(String name) throws JMSException {
        checkMap();
        return map.getChar(name);
    }

    /**
     * Return the integer value with the given name.
     *
     * @param name the name of the integer
     * @return the integer value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public int getInt(String name) throws JMSException {
        checkMap();
        return map.getInt(name);
    }

    /**
     * Return the long value with the given name.
     *
     * @param name the name of the long
     * @return the long value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public long getLong(String name) throws JMSException {
        checkMap();
        return map.getLong(name);
    }

    /**
     * Return the float value with the given name.
     *
     * @param name the name of the float
     * @return the float value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public float getFloat(String name) throws JMSException {
        checkMap();
        return map.getFloat(name);
    }

    /**
     * Return the double value with the given name.
     *
     * @param name the name of the double
     * @return the double value with the given name.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public double getDouble(String name) throws JMSException {
        checkMap();
        return map.getDouble(name);
    }

    /**
     * Return the String value with the given name.
     *
     * @param name the name of the String
     * @return the String value with the given name. If there is no item
     * by this name, a null value is returned.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public String getString(String name) throws JMSException {
        checkMap();
        return map.getString(name);
    }

    /**
     * Return the byte array value with the given name.
     *
     * @param name the name of the byte array
     * @return the byte array value with the given name. If there is no
     * item by this name, a null value is returned.
     * @throws JMSException           if JMS fails to read message due to
     *                                some internal JMS error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public byte[] getBytes(String name) throws JMSException {
        checkMap();
        return map.getBytes(name);
    }

    /**
     * Return the Java object value with the given name.
     * <p/>
     * <P>Note that this method can be used to return in objectified format,
     * an object that had been stored in the Map with the equivalent
     * <CODE>setObject</CODE> method call, or it's equivalent primitive
     * set<type> method.
     *
     * @param name the name of the Java object
     * @return the Java object value with the given name, in objectified
     * format (ie. if it set as an int, then a Integer is returned).
     * If there is no item by this name, a null value is returned.
     * @throws JMSException if JMS fails to read message due to
     *                      some internal JMS error.
     */
    public Object getObject(String name) throws JMSException {
        checkMap();
        return map.getObject(name);
    }

    /**
     * Return an Enumeration of all the Map message's names.
     *
     * @return an enumeration of all the names in this Map message.
     * @throws JMSException if JMS fails to read message due to
     *                      some internal JMS error.
     */
    public Enumeration getMapNames() throws JMSException {
        checkMap();
        return map.enumeration();
    }

    /**
     * Set a boolean value with the given name, into the Map.
     *
     * @param name  the name of the boolean
     * @param value the boolean value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setBoolean(String name, boolean value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setBoolean(name, value);
    }

    /**
     * Set a byte value with the given name, into the Map.
     *
     * @param name  the name of the byte
     * @param value the byte value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setByte(String name, byte value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setByte(name, value);
    }

    /**
     * Set a short value with the given name, into the Map.
     *
     * @param name  the name of the short
     * @param value the short value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setShort(String name, short value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setShort(name, value);
    }

    /**
     * Set a Unicode character value with the given name, into the Map.
     *
     * @param name  the name of the Unicode character
     * @param value the Unicode character value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setChar(String name, char value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setChar(name, value);
    }

    /**
     * Set an integer value with the given name, into the Map.
     *
     * @param name  the name of the integer
     * @param value the integer value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setInt(String name, int value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setInt(name, value);
    }

    /**
     * Set a long value with the given name, into the Map.
     *
     * @param name  the name of the long
     * @param value the long value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setLong(String name, long value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setLong(name, value);
    }

    /**
     * Set a float value with the given name, into the Map.
     *
     * @param name  the name of the float
     * @param value the float value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setFloat(String name, float value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setFloat(name, value);
    }

    /**
     * Set a double value with the given name, into the Map.
     *
     * @param name  the name of the double
     * @param value the double value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setDouble(String name, double value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setDouble(name, value);
    }

    /**
     * Set a String value with the given name, into the Map.
     *
     * @param name  the name of the String
     * @param value the String value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setString(String name, String value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        map.setString(name, value);
    }

    /**
     * Set a byte array value with the given name, into the Map.
     *
     * @param name  the name of the byte array
     * @param value the byte array value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setBytes(String name, byte[] value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        if (value == null)
            map.remove(name);
        else
            map.setBytes(name, value);
    }

    /**
     * Set a portion of the byte array value with the given name, into the Map.
     *
     * @param name   the name of the byte array
     * @param value  the byte array value to set in the Map.
     * @param offset the initial offset within the byte array.
     * @param length the number of bytes to use.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setBytes(String name, byte[] value, int offset,
                         int length) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        if (value == null)
            map.remove(name);
        else
            map.setBytes(name, value, offset, length);
    }

    /**
     * Set a Java object value with the given name, into the Map.
     * <p/>
     * <P>Note that this method only works for the objectified primitive
     * object types (Integer, Double, Long ...), String's and byte arrays.
     *
     * @param name  the name of the Java object
     * @param value the Java object value to set in the Map.
     * @throws JMSException                 if JMS fails to write message due to
     *                                      some internal JMS error.
     * @throws MessageFormatException       if object is invalid
     * @throws MessageNotWriteableException if message in read-only mode.
     */
    public void setObject(String name, Object value) throws JMSException {
        if (bodyReadOnly)
            throw new MessageNotWriteableException("Message values are read only");
        checkMap();
        if (value == null)
            map.remove(name);
        else
            map.setObject(name, value, true);
    }

    /**
     * Check if an item exists in this MapMessage.
     *
     * @param name the name of the item to test
     * @return true if the item does exist.
     * @throws JMSException if a JMS error occurs.
     */
    public boolean itemExists(String name) throws JMSException {
        checkMap();
        return map.exists(name);
    }

    /**
     * Clear out the message body. All other parts of the message are left
     * untouched.
     *
     * @throws JMSException if JMS fails to due to some internal JMS error.
     */
    public void clearBody() throws JMSException {
        map = null;
        mapBytes = null;
        bodyReadOnly = false;
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[MapMessageImpl ");
        b.append(super.toString());
        b.append(" map=");
        b.append(map);
        b.append(" mapBytes=");
        b.append(mapBytes);
        b.append("]");
        return b.toString();
    }
}



