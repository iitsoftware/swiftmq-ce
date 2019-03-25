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

import com.swiftmq.amqp.v100.types.AMQPBinary;

/**
 * <p>
 * </p><p>
 * A transaction-id may be up to 32 octets of binary data.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TransactionId extends AMQPBinary
        implements TxnIdIF {


    /**
     * Constructs a TransactionId.
     *
     * @param initValue initial value
     */
    public TransactionId(byte[] initValue) {
        super(initValue);
    }

    /**
     * Accept method for a TxnId visitor.
     *
     * @param visitor TxnId visitor
     */
    public void accept(TxnIdVisitor visitor) {
        visitor.visit(this);
    }


    public String toString() {
        return "[TransactionId " + super.toString() + "]";
    }
}
