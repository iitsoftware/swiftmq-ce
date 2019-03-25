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

/**
 * <p>
 * </p><p>
 * IETF language tags are abbreviated language codes as defined in the IETF Best Current
 * Practice BCP-47 [ BCP47 ]
 * (incorporating IETF RFC-5646 [ RFC5646 ]).
 * A list of registered subtags is maintained in the IANA Language Subtag Registry
 * [ IANASUBTAG ].
 * </p><p>
 * </p><p>
 * All AMQP implementations should understand at the least the IETF language tag
 * en-US  (note that this uses a hyphen separator, not an underscore).
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class IetfLanguageTag extends AMQPSymbol {


    /**
     * Constructs a IetfLanguageTag.
     *
     * @param initValue initial value
     */
    public IetfLanguageTag(String initValue) {
        super(initValue);
    }


    public String toString() {
        return "[IetfLanguageTag " + super.toString() + "]";
    }
}
