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

import com.swiftmq.amqp.v100.types.AMQPMap;

import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * </p><p>
 * The  fields  type is a map where the keys are restricted to be of type   (this excludes the possibility of a null key). There is no further
 * restriction implied by the  fields  type on the allowed values for the entries or the
 * set of allowed keys.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Fields extends AMQPMap {


    /**
     * Constructs a Fields.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Fields(Map initValue) throws IOException {
        super(initValue);
    }


    public String toString() {
        return "[Fields " + super.toString() + "]";
    }
}
