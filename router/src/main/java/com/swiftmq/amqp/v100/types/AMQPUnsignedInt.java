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
 * Integer in the range 0 to 2^32-1 inclusive
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPUnsignedInt extends AMQPType {
    byte[] bytes = new byte[4];

    /**
     * Constructs an AMQPUnsignedInt with an undefined value
     */
    public AMQPUnsignedInt() {
        super("uint", AMQPTypeDecoder.UINT);
    }

    /**
     * Constructs an AMQPUnsignedInt with a value
     *
     * @param value value
     */
    public AMQPUnsignedInt(long value) {
        super("uint", AMQPTypeDecoder.UINT);
        setValue(value);
    }

    /**
     * Sets the value
     *
     * @param value value
     */
    public void setValue(long value) {
        Util.writeInt((int) value, bytes, 0);
        code = value == 0 ? AMQPTypeDecoder.UINT0 : AMQPTypeDecoder.UINT;
    }

    /**
     * Returns the value
     *
     * @return value
     */
    public long getValue() {
        if (code == AMQPTypeDecoder.UINT)
            return Util.readInt(bytes, 0) & 0xffffffffL;
        if (code == AMQPTypeDecoder.SUINT)
            return bytes[0] & 0xff;
        return 0;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize();
        if (code == AMQPTypeDecoder.UINT)
            n += 4;
        else if (code == AMQPTypeDecoder.SUINT)
            n += 1;
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        if (code == AMQPTypeDecoder.UINT)
            in.readFully(bytes);
        else if (code == AMQPTypeDecoder.SUINT)
            bytes[0] = in.readByte();
        else if (code != AMQPTypeDecoder.UINT0)
            throw new IOException("Invalid code: " + code);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (code == AMQPTypeDecoder.UINT)
            out.write(bytes);
        else if (code == AMQPTypeDecoder.SUINT)
            out.writeByte(bytes[0]);
    }

    public String getValueString() {
        return Long.toString(getValue());
    }

    public String toString() {
        return "[AMQPUnsignedInt, value=" + getValue() + " " + super.toString() + "]";
    }
}