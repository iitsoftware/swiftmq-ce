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

package com.swiftmq.impl.streams.comp.message;

import com.swiftmq.jms.StreamMessageImpl;

import javax.jms.JMSException;

/**
 * Facade to wrap the body of a javax.jms.StreamMessage
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class StreamBody {
    StreamMessage message;

    StreamBody(StreamMessage message) {
        this.message = message;
    }

    /**
     * Reads a boolean.
     *
     * @return boolean
     * @throws JMSException
     */
    public boolean readBoolean() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readBoolean();
    }

    /**
     * Reads a byte.
     *
     * @return byte
     * @throws JMSException
     */
    public byte readByte() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readByte();
    }

    /**
     * Reads a char.
     *
     * @return char
     * @throws JMSException
     */
    public char readChar() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readChar();
    }

    /**
     * Reads into an array of bytes.
     *
     * @param bytes array of bytes
     * @throws JMSException
     */
    public void readBytes(byte[] bytes) throws JMSException {
        ((StreamMessageImpl)message.getImpl()).readBytes(bytes);
    }

    /**
     * Reads a short.
     *
     * @return short
     * @throws JMSException
     */
    public short readShort() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readShort();
    }

    /**
     * Reads an int.
     *
     * @return int
     * @throws JMSException
     */
    public int readInt() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readInt();
    }

    /**
     * Reads a long.
     *
     * @return long
     * @throws JMSException
     */
    public long readLong() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readLong();
    }

    /**
     * Reads a double.
     * @return double
     * @throws JMSException
     */
    public double readDouble() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readDouble();
    }

    /**
     * Reads a float.
     *
     * @return float.
     * @throws JMSException
     */
    public float readFloat() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readFloat();
    }

    /**
     * Reads a string.
     *
     * @return string
     * @throws JMSException
     */
    public String readString() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readString();
    }

    /**
     * Reads an object.
     *
     * @return object
     * @throws JMSException
     */
    public Object readObject() throws JMSException {
        return ((StreamMessageImpl)message.getImpl()).readObject();
    }

    /**
     * Resets the stream body to the beginning.
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody reset() throws JMSException {
        ((StreamMessageImpl)message.getImpl()).reset();
        return this;
    }

    /**
     * Write a boolean.
     *
     * @param b boolean
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeBoolean(boolean b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeBoolean(b);
        return this;
    }

    /**
     * Write a char.
     *
     * @param b char
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeChar(char b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeChar(b);
        return this;
    }

    /**
     * Write a byte.
     *
     * @param b byte
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeByte(byte b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeByte(b);
        return this;
    }

    /**
     * Write a byte array.
     *
     * @param b byte array
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeBytes(byte[] b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeBytes(b);
        return this;
    }

    /**
     * Write a byte array with offset and length.
     *
     * @param b byte array
     * @param off offset
     * @param len length
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeBytes(byte[] b, int off, int len) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeBytes(b, off, len);
        return this;
    }

    /**
     * Write a short.
     *
     * @param b short
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeShort(short b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeShort(b);
        return this;
    }

    /**
     * Write a int.
     *
     * @param b int
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeInt(int b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeInt(b);
        return this;
    }

    /**
     * Write a long.
     *
     * @param b long
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeLong(long b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeLong(b);
        return this;
    }

    /**
     * Write a double.
     *
     * @param b double
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeDouble(double b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeDouble(b);
        return this;
    }

    /**
     * Write a float.
     *
     * @param b float
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeFloat(float b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeFloat(b);
        return this;
    }

    /**
     * Write a String.
     *
     * @param b String
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeString(String b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeString(b);
        return this;
    }

    /**
     * Write a Object.
     *
     * @param b Object
     * @return StreamBody
     * @throws JMSException
     */
    public StreamBody writeObject(Object b) throws JMSException {
        message.ensureLocalCopy();
        ((StreamMessageImpl)message.getImpl()).writeObject(b);
        return this;
    }

}
