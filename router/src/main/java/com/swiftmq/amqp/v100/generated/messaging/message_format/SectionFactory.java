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

import java.io.IOException;

/**
 * Factory class to create SectionIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class SectionFactory {

    /**
     * Creates a SectionIF object.
     *
     * @param bare the bare AMQP type
     * @return SectionIF
     */
    public static SectionIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new IOException("Missing constructor: " + bare);
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        if (AMQPTypeDecoder.isULong(code)) {
            long type = ((AMQPUnsignedLong) descriptor).getValue();
            if (type == Header.DESCRIPTOR_CODE)
                return new Header(((AMQPList) bare).getValue());
            if (type == DeliveryAnnotations.DESCRIPTOR_CODE)
                return new DeliveryAnnotations(((AMQPMap) bare).getValue());
            if (type == MessageAnnotations.DESCRIPTOR_CODE)
                return new MessageAnnotations(((AMQPMap) bare).getValue());
            if (type == Properties.DESCRIPTOR_CODE)
                return new Properties(((AMQPList) bare).getValue());
            if (type == ApplicationProperties.DESCRIPTOR_CODE)
                return new ApplicationProperties(((AMQPMap) bare).getValue());
            if (type == Data.DESCRIPTOR_CODE)
                return new Data(((AMQPBinary) bare).getValue());
            if (type == AmqpSequence.DESCRIPTOR_CODE)
                return new AmqpSequence(((AMQPList) bare).getValue());
            if (type == AmqpValue.DESCRIPTOR_CODE)
                return new AmqpValue(bare);
            if (type == Footer.DESCRIPTOR_CODE)
                return new Footer(((AMQPMap) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else if (AMQPTypeDecoder.isSymbol(code)) {
            String type = ((AMQPSymbol) descriptor).getValue();
            if (type.equals(Header.DESCRIPTOR_NAME))
                return new Header(((AMQPList) bare).getValue());
            if (type.equals(DeliveryAnnotations.DESCRIPTOR_NAME))
                return new DeliveryAnnotations(((AMQPMap) bare).getValue());
            if (type.equals(MessageAnnotations.DESCRIPTOR_NAME))
                return new MessageAnnotations(((AMQPMap) bare).getValue());
            if (type.equals(Properties.DESCRIPTOR_NAME))
                return new Properties(((AMQPList) bare).getValue());
            if (type.equals(ApplicationProperties.DESCRIPTOR_NAME))
                return new ApplicationProperties(((AMQPMap) bare).getValue());
            if (type.equals(Data.DESCRIPTOR_NAME))
                return new Data(((AMQPBinary) bare).getValue());
            if (type.equals(AmqpSequence.DESCRIPTOR_NAME))
                return new AmqpSequence(((AMQPList) bare).getValue());
            if (type.equals(AmqpValue.DESCRIPTOR_NAME))
                return new AmqpValue(bare);
            if (type.equals(Footer.DESCRIPTOR_NAME))
                return new Footer(((AMQPMap) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else
            throw new Exception("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare);
    }

    /**
     * Converts an AMQP array of type SectionIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static SectionIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        SectionIF[] n = new SectionIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
