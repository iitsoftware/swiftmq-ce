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

package com.swiftmq.amqp.v100.generated.messaging.message_format;

import com.swiftmq.amqp.v100.types.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 * </p><p>
 * An amqp-value section contains a single AMQP value.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class AmqpValue extends AMQPType
        implements SectionIF {
    public static String DESCRIPTOR_NAME = "amqp:amqp-value:*";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000077L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    AMQPType value = null;


    /**
     * Constructs a AmqpValue.
     *
     * @param initValue initial value
     */
    public AmqpValue(AMQPType initValue) {
        super("*", AMQPTypeDecoder.UNKNOWN);
        value = initValue;
    }

    /**
     * Returns the value of this AmqpValue.
     *
     * @return value
     */
    public AMQPType getValue() {
        return value;
    }

    /**
     * Accept method for a Section visitor.
     *
     * @param visitor Section visitor
     */
    public void accept(SectionVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return whether this AmqpValue has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    public void writeContent(DataOutput out) throws IOException {
        codeConstructor.setFormatCode(AMQPTypeDecoder.UNKNOWN);
        codeConstructor.writeContent(out);
        value.writeContent(out);
    }

    /**
     * Returns the predicted size of this AmqpValue. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n = value.getPredictedSize();
        if (getConstructor() == null)
            n += codeConstructor.getPredictedSize();
        return n;
    }

    public void readContent(DataInput in) throws IOException {
        // do nothing
        throw new IOException("Invalid operation, readContent not implemented!");
    }

    public String getValueString() {
        StringBuffer b = new StringBuffer("[AmqpValue ");
        b.append(value.getValueString());
        b.append("]");
        return b.toString();
    }

    public String toString() {
        return "[AmqpValue " + value.toString() + "]";
    }
}
