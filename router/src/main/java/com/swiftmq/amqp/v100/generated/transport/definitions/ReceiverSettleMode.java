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

import com.swiftmq.amqp.v100.types.AMQPUnsignedByte;

import java.util.HashSet;
import java.util.Set;

/**
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class ReceiverSettleMode extends AMQPUnsignedByte {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add(0);
        POSSIBLE_VALUES.add(1);
    }

    public static final ReceiverSettleMode FIRST = new ReceiverSettleMode(0);
    public static final ReceiverSettleMode SECOND = new ReceiverSettleMode(1);

    /**
     * Constructs a ReceiverSettleMode.
     *
     * @param initValue initial value
     */
    public ReceiverSettleMode(int initValue) {
        super(initValue);
    }


    public String toString() {
        return "[ReceiverSettleMode " + super.toString() + "]";
    }
}
