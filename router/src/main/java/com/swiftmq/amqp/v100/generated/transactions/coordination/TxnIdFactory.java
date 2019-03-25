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

package com.swiftmq.amqp.v100.generated.transactions.coordination;

import com.swiftmq.amqp.v100.types.AMQPArray;
import com.swiftmq.amqp.v100.types.AMQPBinary;
import com.swiftmq.amqp.v100.types.AMQPType;
import com.swiftmq.amqp.v100.types.AMQPTypeDecoder;

/**
 * Factory class to create TxnIdIF objects out of a bare AMQPType
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TxnIdFactory {

    /**
     * Creates a TxnIdIF object.
     *
     * @param bare the bare AMQP type
     * @return TxnIdIF
     */
    public static TxnIdIF create(AMQPType bare) throws Exception {
        if (bare.getCode() == AMQPTypeDecoder.NULL)
            return null;
        return new TransactionId(((AMQPBinary) bare).getValue());
    }

    /**
     * Converts an AMQP array of type TxnIdIF into a native array
     *
     * @param array AMQP array
     * @return native array
     */
    public static TxnIdIF[] toNativeArray(AMQPArray array) throws Exception {
        if (array == null)
            return null;
        AMQPType[] value = array.getValue();
        if (value == null)
            return null;
        TxnIdIF[] n = new TxnIdIF[value.length];
        for (int i = 0; i < value.length; i++)
            n[i] = create(value[i]);
        return n;
    }
}
