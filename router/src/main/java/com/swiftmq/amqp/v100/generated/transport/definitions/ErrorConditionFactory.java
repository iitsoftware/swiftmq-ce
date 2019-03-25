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

package com.swiftmq.amqp.v100.generated.transport.definitions;

import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionError;
import com.swiftmq.amqp.v100.types.AMQPArray;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import com.swiftmq.amqp.v100.types.AMQPType;
import com.swiftmq.amqp.v100.types.AMQPTypeDecoder;

/**
 * Factory class to create ErrorConditionIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class ErrorConditionFactory {

    /**
     * Creates a ErrorConditionIF object.
     *
     * @param bare the bare AMQP type
     * @return ErrorConditionIF
     */
    public static ErrorConditionIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        int type = bare.getCode();
        if (AmqpError.POSSIBLE_VALUES.contains(((AMQPSymbol) bare).getValue()))
            return new AmqpError(((AMQPSymbol) bare).getValue());
        if (ConnectionError.POSSIBLE_VALUES.contains(((AMQPSymbol) bare).getValue()))
            return new ConnectionError(((AMQPSymbol) bare).getValue());
        if (SessionError.POSSIBLE_VALUES.contains(((AMQPSymbol) bare).getValue()))
            return new SessionError(((AMQPSymbol) bare).getValue());
        if (LinkError.POSSIBLE_VALUES.contains(((AMQPSymbol) bare).getValue()))
            return new LinkError(((AMQPSymbol) bare).getValue());
        if (TransactionError.POSSIBLE_VALUES.contains(((AMQPSymbol) bare).getValue()))
            return new TransactionError(((AMQPSymbol) bare).getValue());
        throw new Exception("Invalid type: " + type + ", bare=" + bare);
    }

    /**
     * Converts an AMQP array of type ErrorConditionIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static ErrorConditionIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        ErrorConditionIF[] n = new ErrorConditionIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
