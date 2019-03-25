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
 * Integer in the range -(2^7) to 2^7-1 inclusive
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPByte extends AMQPType {
    byte value;

    /**
     * Constructs an AMQPByte with an undefined value
     */
    public AMQPByte() {
        super("byte", AMQPTypeDecoder.BYTE);
    }

    /**
     * Constructs an AMQPByte with a value
     *
     * @param value value
     */
    public AMQPByte(byte value) {
        super("byte", AMQPTypeDecoder.BYTE);
        setValue(value);
    }

    /**
     * Sets the value
     *
     * @param value value
     */
    public void setValue(byte value) {
        this.value = value;
    }

    /**
     * Returns the value
     *
     * @return value
     */
    public byte getValue() {
        return value;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize() + 1;
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        value = in.readByte();
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.writeByte(value);
    }

    public String getValueString() {
        return Byte.toString(value);
    }

    public String toString() {
        return "[AMQPByte, value=" + getValue() + " " + super.toString() + "]";
    }
}