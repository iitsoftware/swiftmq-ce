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

import com.swiftmq.amqp.v100.types.AMQPDescribedConstructor;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import com.swiftmq.amqp.v100.types.AMQPTypeDecoder;
import com.swiftmq.amqp.v100.types.AMQPUnsignedLong;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * </p><p>
 * The delivery-annotations section is used for delivery-specific non-standard properties at
 * the head of the message. Delivery annotations convey information from the sending peer to
 * the receiving peer. If the recipient does not understand the annotation it cannot be acted
 * upon and its effects (such as any implied propagation) cannot be acted upon. Annotations
 * may be specific to one implementation, or common to multiple implementations. The
 * capabilities negotiated on link   and on the   and
 * should be used to establish which annotations a peer supports. A
 * registry of defined annotations and their meanings is maintained
 * [ AMQPDELANN ]. The symbolic key "rejected" is reserved for
 * the use of communicating error information regarding rejected messages. Any values
 * associated with the "rejected" key MUST be of type  .
 * </p><p>
 * </p><p>
 * If the delivery-annotations section is omitted, it is equivalent to a delivery-annotations
 * section containing an empty map of annotations.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class DeliveryAnnotations extends Annotations
        implements SectionIF {
    public static String DESCRIPTOR_NAME = "amqp:delivery-annotations:map";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000071L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);


    /**
     * Constructs a DeliveryAnnotations.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public DeliveryAnnotations(Map initValue) throws IOException {
        super(initValue);
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
     * Return whether this DeliveryAnnotations has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    public void writeContent(DataOutput out) throws IOException {
        if (getConstructor() != codeConstructor) {
            codeConstructor.setFormatCode(getCode());
            setConstructor(codeConstructor);
        }
        super.writeContent(out);
    }

    public String toString() {
        return "[DeliveryAnnotations " + super.toString() + "]";
    }
}
