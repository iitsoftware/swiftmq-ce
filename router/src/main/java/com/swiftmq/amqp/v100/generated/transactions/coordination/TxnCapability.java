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

import com.swiftmq.amqp.v100.types.AMQPSymbol;

import java.util.HashSet;
import java.util.Set;

/**
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TxnCapability extends AMQPSymbol
        implements TxnCapabilityIF {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add("amqp:local-transactions");
        POSSIBLE_VALUES.add("amqp:distributed-transactions");
        POSSIBLE_VALUES.add("amqp:promotable-transactions");
        POSSIBLE_VALUES.add("amqp:multi-txns-per-ssn");
        POSSIBLE_VALUES.add("amqp:multi-ssns-per-txn");
    }

    public static final TxnCapability LOCAL_TRANSACTIONS = new TxnCapability("amqp:local-transactions");
    public static final TxnCapability DISTRIBUTED_TRANSACTIONS = new TxnCapability("amqp:distributed-transactions");
    public static final TxnCapability PROMOTABLE_TRANSACTIONS = new TxnCapability("amqp:promotable-transactions");
    public static final TxnCapability MULTI_TXNS_PER_SSN = new TxnCapability("amqp:multi-txns-per-ssn");
    public static final TxnCapability MULTI_SSNS_PER_TXN = new TxnCapability("amqp:multi-ssns-per-txn");

    /**
     * Constructs a TxnCapability.
     *
     * @param initValue initial value
     */
    public TxnCapability(String initValue) {
        super(initValue);
    }

    /**
     * Accept method for a TxnCapability visitor.
     *
     * @param visitor TxnCapability visitor
     */
    public void accept(TxnCapabilityVisitor visitor) {
        visitor.visit(this);
    }


    public String toString() {
        return "[TxnCapability " + super.toString() + "]";
    }
}
