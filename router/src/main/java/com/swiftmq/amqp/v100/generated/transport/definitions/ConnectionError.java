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

import com.swiftmq.amqp.v100.types.AMQPSymbol;

import java.util.HashSet;
import java.util.Set;

/**
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class ConnectionError extends AMQPSymbol
        implements ErrorConditionIF {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add("amqp:connection:forced");
        POSSIBLE_VALUES.add("amqp:connection:framing-error");
        POSSIBLE_VALUES.add("amqp:connection:redirect");
    }

    public static final ConnectionError CONNECTION_FORCED = new ConnectionError("amqp:connection:forced");
    public static final ConnectionError FRAMING_ERROR = new ConnectionError("amqp:connection:framing-error");
    public static final ConnectionError REDIRECT = new ConnectionError("amqp:connection:redirect");

    /**
     * Constructs a ConnectionError.
     *
     * @param initValue initial value
     */
    public ConnectionError(String initValue) {
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
        return "[ConnectionError " + super.toString() + "]";
    }
}
