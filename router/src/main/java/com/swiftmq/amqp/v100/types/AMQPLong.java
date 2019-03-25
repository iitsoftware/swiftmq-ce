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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Integer in the range -(2^63) to 2^63-1 inclusive
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPLong extends AMQPType {
    byte[] bytes = new byte[8];

    /**
     * Constructs an AMQPLong with an undefined value
     */
    public AMQPLong() {
        super("long", AMQPTypeDecoder.LONG);
    }

    /**
     * Constructs an AMQPLong with a value
     *
     * @param value value
     */
    public AMQPLong(long value) {
        super("long", AMQPTypeDecoder.LONG);
        setValue(value);
    }

    /**
     * Sets the value
     *
     * @param value value
     */
    public void setValue(long value) {
        Util.writeLong(value, bytes, 0);
        code = AMQPTypeDecoder.LONG;
    }

    /**
     * Returns the value
     *
     * @return value
     */
    public long getValue() {
        if (code == AMQPTypeDecoder.LONG)
            return Util.readLong(bytes, 0);
        return (long) bytes[0];
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (code == AMQPTypeDecoder.LONG)
            n += 8;
        else if (code == AMQPTypeDecoder.SLONG)
            n += 1;
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        if (code == AMQPTypeDecoder.LONG)
            in.readFully(bytes);
        else if (code == AMQPTypeDecoder.SLONG)
            bytes[0] = in.readByte();
        else
            throw new IOException("Invalid code: " + code);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (code == AMQPTypeDecoder.LONG)
            out.write(bytes);
        else
            out.writeByte(bytes[0]);
    }

    public String getValueString() {
        return Long.toString(getValue());
    }

    public String toString() {
        return "[AMQPLong, value=" + getValue() + " " + super.toString() + "]";
    }
}