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

package com.swiftmq.amqp.v100.generated.messaging.message_format;

import com.swiftmq.amqp.v100.types.AMQPMap;

import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * </p><p>
 * The  annotations  type is a map where the keys are restricted to be of type   or of type  . All ulong keys,
 * and all symbolic keys except those beginning with "x-" are reserved. Keys beginning with
 * "x-opt-" MUST be ignored if not understood. On receiving an annotation key which is not
 * understood, and which does not begin with "x-opt", the receiving AMQP container MUST
 * detach the link with a   error.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Annotations extends AMQPMap {


    /**
     * Constructs a Annotations.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Annotations(Map initValue) throws IOException {
        super(initValue);
    }


    public String toString() {
        return "[Annotations " + super.toString() + "]";
    }
}
