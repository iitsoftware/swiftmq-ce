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

import com.swiftmq.amqp.v100.types.AMQPSymbol;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * </p><p>
 * Policies for distributing messages when multiple links are connected to the same node.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class StdDistMode extends AMQPSymbol
        implements DistributionModeIF {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add("move");
        POSSIBLE_VALUES.add("copy");
    }

    public static final StdDistMode MOVE = new StdDistMode("move");
    public static final StdDistMode COPY = new StdDistMode("copy");

    /**
     * Constructs a StdDistMode.
     *
     * @param initValue initial value
     */
    public StdDistMode(String initValue) {
        super(initValue);
    }

    /**
     * Accept method for a DistributionMode visitor.
     *
     * @param visitor DistributionMode visitor
     */
    public void accept(DistributionModeVisitor visitor) {
        visitor.visit(this);
    }


    public String toString() {
        return "[StdDistMode " + super.toString() + "]";
    }
}
