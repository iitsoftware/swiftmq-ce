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
 * Determines when the expiry timer of a terminus starts counting down from the timeout
 * value. If the link is subsequently re-attached before the terminus is expired, then the
 * count down is aborted. If the conditions for the terminus-expiry-policy are subsequently
 * re-met, the expiry timer restarts from its originally configured timeout value.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class TerminusExpiryPolicy extends AMQPSymbol {

    public static final Set POSSIBLE_VALUES = new HashSet();

    static {
        POSSIBLE_VALUES.add("link-detach");
        POSSIBLE_VALUES.add("session-end");
        POSSIBLE_VALUES.add("connection-close");
        POSSIBLE_VALUES.add("never");
    }

    public static final TerminusExpiryPolicy LINK_DETACH = new TerminusExpiryPolicy("link-detach");
    public static final TerminusExpiryPolicy SESSION_END = new TerminusExpiryPolicy("session-end");
    public static final TerminusExpiryPolicy CONNECTION_CLOSE = new TerminusExpiryPolicy("connection-close");
    public static final TerminusExpiryPolicy NEVER = new TerminusExpiryPolicy("never");

    /**
     * Constructs a TerminusExpiryPolicy.
     *
     * @param initValue initial value
     */
    public TerminusExpiryPolicy(String initValue) {
        super(initValue);
    }


    public String toString() {
        return "[TerminusExpiryPolicy " + super.toString() + "]";
    }
}
