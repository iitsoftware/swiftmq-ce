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

package com.swiftmq.amqp.v100.generated.messaging.delivery_state;

import com.swiftmq.amqp.v100.generated.transactions.coordination.Declared;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionalState;
import com.swiftmq.amqp.v100.types.*;

import java.io.IOException;

/**
 * Factory class to create DeliveryStateIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class DeliveryStateFactory {

    /**
     * Creates a DeliveryStateIF object.
     *
     * @param bare the bare AMQP type
     * @return DeliveryStateIF
     */
    public static DeliveryStateIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new IOException("Missing constructor: " + bare);
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        if (AMQPTypeDecoder.isULong(code)) {
            long type = ((AMQPUnsignedLong) descriptor).getValue();
            if (type == Received.DESCRIPTOR_CODE)
                return new Received(((AMQPList) bare).getValue());
            if (type == Accepted.DESCRIPTOR_CODE)
                return new Accepted(((AMQPList) bare).getValue());
            if (type == Rejected.DESCRIPTOR_CODE)
                return new Rejected(((AMQPList) bare).getValue());
            if (type == Released.DESCRIPTOR_CODE)
                return new Released(((AMQPList) bare).getValue());
            if (type == Modified.DESCRIPTOR_CODE)
                return new Modified(((AMQPList) bare).getValue());
            if (type == Declared.DESCRIPTOR_CODE)
                return new Declared(((AMQPList) bare).getValue());
            if (type == TransactionalState.DESCRIPTOR_CODE)
                return new TransactionalState(((AMQPList) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else if (AMQPTypeDecoder.isSymbol(code)) {
            String type = ((AMQPSymbol) descriptor).getValue();
            if (type.equals(Received.DESCRIPTOR_NAME))
                return new Received(((AMQPList) bare).getValue());
            if (type.equals(Accepted.DESCRIPTOR_NAME))
                return new Accepted(((AMQPList) bare).getValue());
            if (type.equals(Rejected.DESCRIPTOR_NAME))
                return new Rejected(((AMQPList) bare).getValue());
            if (type.equals(Released.DESCRIPTOR_NAME))
                return new Released(((AMQPList) bare).getValue());
            if (type.equals(Modified.DESCRIPTOR_NAME))
                return new Modified(((AMQPList) bare).getValue());
            if (type.equals(Declared.DESCRIPTOR_NAME))
                return new Declared(((AMQPList) bare).getValue());
            if (type.equals(TransactionalState.DESCRIPTOR_NAME))
                return new TransactionalState(((AMQPList) bare).getValue());
            throw new Exception("Invalid descriptor type: " + type + ", bare=" + bare);
        } else
            throw new Exception("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare);
    }

    /**
     * Converts an AMQP array of type DeliveryStateIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static DeliveryStateIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        DeliveryStateIF[] n = new DeliveryStateIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
