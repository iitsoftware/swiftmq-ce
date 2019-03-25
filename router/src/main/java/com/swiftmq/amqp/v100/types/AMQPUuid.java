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
import java.util.UUID;

/**
 * A universally unique id as defined by RFC-4122 section 4.1.2
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPUuid extends AMQPType {
    byte[] bytes;

    /**
     * Constructs an AMQPUuid with an undefined value
     */
    public AMQPUuid() {
        super("uuid", AMQPTypeDecoder.UUID);
    }

    /**
     * Constructs an AMQPUuid with an UUID value
     *
     * @param value value
     */
    public AMQPUuid(UUID value) {
        this(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    /**
     * Constructs an AMQPUuid with 2 longs.
     *
     * @param mostSigBits  most significant bits
     * @param leastSigBits least significant bits
     */
    public AMQPUuid(long mostSigBits, long leastSigBits) {
        super("uuid", AMQPTypeDecoder.UUID);
        setValue(mostSigBits, leastSigBits);
    }

    /**
     * Sets the value.
     *
     * @param mostSigBits  most significant bits
     * @param leastSigBits least significant bits
     */
    public void setValue(long mostSigBits, long leastSigBits) {
        bytes = new byte[16];
        Util.writeLong(mostSigBits, bytes, 0);
        Util.writeLong(leastSigBits, bytes, 4);
    }

    /**
     * Returns the value
     *
     * @return UUID
     */
    public UUID getValue() {
        return new UUID(Util.readLong(bytes, 0), Util.readLong(bytes, 4));
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize() + 16;
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        bytes = new byte[16];
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
        return "[AMQPUuid, UUID=" + getValue() + ", bytes.length=" + (bytes == null ? "null" : bytes.length) + " " + super.toString() + "]";
    }
}
