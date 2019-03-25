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
 * The message-annotations section is used for properties of the message which are aimed at
 * the infrastructure and should be propagated across every delivery step. Message
 * annotations convey information about the message. Intermediaries MUST propagate the
 * annotations unless the annotations are explicitly augmented or modified (e.g., by the use
 * of the   outcome).
 * </p><p>
 * </p><p>
 * The capabilities negotiated on link   and on the
 * and   may be used to establish which annotations a peer understands;
 * however, in a network of AMQP intermediaries it may not be possible to know if every
 * intermediary will understand the annotation. Note that for some annotations it may not be
 * necessary for the intermediary to understand their purpose, i.e., they could be
 * used purely as an attribute which can be filtered on.
 * </p><p>
 * </p><p>
 * A registry of defined annotations and their meanings is maintained
 * [ AMQPMESSANN ].
 * </p><p>
 * </p><p>
 * If the message-annotations section is omitted, it is equivalent to a message-annotations
 * section containing an empty map of annotations.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class MessageAnnotations extends Annotations
        implements SectionIF {
    public static String DESCRIPTOR_NAME = "amqp:message-annotations:map";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000072L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);


    /**
     * Constructs a MessageAnnotations.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public MessageAnnotations(Map initValue) throws IOException {
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
     * Return whether this MessageAnnotations has a descriptor
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
        return "[MessageAnnotations " + super.toString() + "]";
    }
}
