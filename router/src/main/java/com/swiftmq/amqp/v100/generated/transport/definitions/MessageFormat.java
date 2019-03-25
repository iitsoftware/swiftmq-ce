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
 * The upper three octets of a message format code identify a particular message format. The
 * lowest octet indicates the version of said message format. Any given version of a format
 * is forwards compatible with all higher versions.
 * </p><p>
 * </p><p>
 * 3 octets      1 octet
 * +----------------+---------+
 * | message format | version |
 * +----------------+---------+
 * |                          |
 * msb                        lsb
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class MessageFormat extends AMQPUnsignedInt {


    /**
     * Constructs a MessageFormat.
     *
     * @param initValue initial value
     */
    public MessageFormat(long initValue) {
        super(initValue);
    }


    public String toString() {
        return "[MessageFormat " + super.toString() + "]";
    }
}
