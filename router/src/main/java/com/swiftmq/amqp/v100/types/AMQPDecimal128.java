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
 * 128-bit decimal number (IEEE 754-2008 decimal128)
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPDecimal128 extends AMQPType {
    byte[] bytes = new byte[16];

    /**
     * Constructs an AMQPDecimal128 with an undefined value
     */
    public AMQPDecimal128() {
        super("decimal128", AMQPTypeDecoder.DECIMAL128);
    }

    /**
     * Constructs an AMQPDecimal128 with a value
     *
     * @param value value
     */
    public AMQPDecimal128(byte[] value) {
        super("decimal128", AMQPTypeDecoder.DECIMAL128);
        setValue(value);
    }

    /**
     * Sets the value
     *
     * @param value value
     */
    public void setValue(byte[] value) {
        System.arraycopy(value, 0, bytes, 0, Math.min(value.length, bytes.length));
    }

    /**
     * Returns the value
     *
     * @return value
     */
    public byte[] getValue() {
        return bytes;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize() + bytes.length;
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        in.readFully(bytes);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.write(bytes);
    }

    public String getValueString() {
        return getValue().toString();
    }

    public String toString() {
        return "[AMQPDecimal128, value=" + getValue() + " " + super.toString() + "]";
    }
}