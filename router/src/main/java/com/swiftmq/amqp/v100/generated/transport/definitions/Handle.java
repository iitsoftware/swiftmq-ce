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

import com.swiftmq.amqp.v100.types.AMQPUnsignedInt;

/**
 * <p>
 * </p><p>
 * An alias established by the   frame and subsequently used
 * by endpoints as a shorthand to refer to the link in all outgoing frames. The two endpoints
 * may potentially use different handles to refer to the same link. Link handles may be
 * reused once a link is closed for both send and receive.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Handle extends AMQPUnsignedInt {


    /**
     * Constructs a Handle.
     *
     * @param initValue initial value
     */
    public Handle(long initValue) {
        super(initValue);
    }


    public String toString() {
        return "[Handle " + super.toString() + "]";
    }
}
