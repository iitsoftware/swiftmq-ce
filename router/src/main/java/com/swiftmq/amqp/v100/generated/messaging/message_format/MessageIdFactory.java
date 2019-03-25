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

/**
 * Factory class to create MessageIdIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class MessageIdFactory {

    /**
     * Creates a MessageIdIF object.
     *
     * @param bare the bare AMQP type
     * @return MessageIdIF
     */
    public static MessageIdIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        int type = bare.getCode();
        if (AMQPTypeDecoder.isULong(type))
            return new MessageIdUlong(((AMQPUnsignedLong) bare).getValue());
        if (type == AMQPTypeDecoder.UUID)
            return new MessageIdUuid(((AMQPUuid) bare).getValue());
        if (AMQPTypeDecoder.isBinary(type))
            return new MessageIdBinary(((AMQPBinary) bare).getValue());
        if (AMQPTypeDecoder.isString(type))
            return new MessageIdString(((AMQPString) bare).getValue());
        throw new Exception("Invalid type: " + type + ", bare=" + bare);
    }

    /**
     * Converts an AMQP array of type MessageIdIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static MessageIdIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        MessageIdIF[] n = new MessageIdIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
