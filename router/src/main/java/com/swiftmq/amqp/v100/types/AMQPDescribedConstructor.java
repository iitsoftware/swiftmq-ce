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
 * A described type constructor.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPDescribedConstructor extends AMQPType {
    AMQPType descriptor = null;
    int formatCode = 0;

    /**
     * Constructs an AMQPDescribedConstructor
     *
     * @param descriptor the descriptor
     * @param formatCode format code
     */
    public AMQPDescribedConstructor(AMQPType descriptor, int formatCode) {
        this();
        this.descriptor = descriptor;
        this.formatCode = formatCode;
    }

    /**
     * Constructs an AMQPDescribedConstructor
     */
    public AMQPDescribedConstructor() {
        super("describedconstructor", AMQPTypeDecoder.CONSTRUCTOR);
    }

    /**
     * Returns the descriptor
     *
     * @return descriptor
     */
    public AMQPType getDescriptor() {
        return descriptor;
    }

    /**
     * Returns the format code.
     *
     * @return format code
     */
    public int getFormatCode() {
        return formatCode;
    }

    /**
     * Sets the format code.
     *
     * @param formatCode format code
     */
    public void setFormatCode(int formatCode) {
        this.formatCode = formatCode;
    }

    public int getPredictedSize() {
        int n = super.getPredictedSize() + 1;
        if (descriptor != null)
            n += descriptor.getPredictedSize();
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        descriptor = AMQPTypeDecoder.decode(in);
        formatCode = in.readUnsignedByte();
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        descriptor.writeContent(out);
        if (formatCode != AMQPTypeDecoder.UNKNOWN)
            out.writeByte(formatCode);
    }

    public String getValueString() {
        return descriptor.getValueString() + "/0x" + Integer.toHexString(formatCode);
    }

    public String toString() {
        return "[AMQPDescribedConstructor, descriptor=" + descriptor + ", formatCode=0x" + Integer.toHexString(formatCode) + super.toString() + "]";
    }
}
