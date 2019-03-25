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

import com.swiftmq.amqp.v100.generated.transport.definitions.ErrorConditionIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.ErrorConditionVisitor;
import com.swiftmq.amqp.v100.types.AMQPSymbol;

import java.util.HashSet;
import java.util.Set;

/**
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TransactionError extends AMQPSymbol
        implements ErrorConditionIF {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add("amqp:transaction:unknown-id");
        POSSIBLE_VALUES.add("amqp:transaction:rollback");
        POSSIBLE_VALUES.add("amqp:transaction:timeout");
    }

    public static final TransactionError UNKNOWN_ID = new TransactionError("amqp:transaction:unknown-id");
    public static final TransactionError TRANSACTION_ROLLBACK = new TransactionError("amqp:transaction:rollback");
    public static final TransactionError TRANSACTION_TIMEOUT = new TransactionError("amqp:transaction:timeout");

    /**
     * Constructs a TransactionError.
     *
     * @param initValue initial value
     */
    public TransactionError(String initValue) {
        super(initValue);
    }

    /**
     * Accept method for a ErrorCondition visitor.
     *
     * @param visitor ErrorCondition visitor
     */
    public void accept(ErrorConditionVisitor visitor) {
        visitor.visit(this);
    }


    public String toString() {
        return "[TransactionError " + super.toString() + "]";
    }
}
