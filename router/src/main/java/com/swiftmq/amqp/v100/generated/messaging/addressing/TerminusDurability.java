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

package com.swiftmq.amqp.v100.generated.messaging.addressing;

import com.swiftmq.amqp.v100.types.AMQPUnsignedInt;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * </p><p>
 * Determines which state of the terminus is held durably.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TerminusDurability extends AMQPUnsignedInt {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add(0L);
        POSSIBLE_VALUES.add(1L);
        POSSIBLE_VALUES.add(2L);
    }

    public static final TerminusDurability NONE = new TerminusDurability(0L);
    public static final TerminusDurability CONFIGURATION = new TerminusDurability(1L);
    public static final TerminusDurability UNSETTLED_STATE = new TerminusDurability(2L);

    /**
     * Constructs a TerminusDurability.
     *
     * @param initValue initial value
     */
    public TerminusDurability(long initValue) {
        super(initValue);
    }


    public String toString() {
        return "[TerminusDurability " + super.toString() + "]";
    }
}
