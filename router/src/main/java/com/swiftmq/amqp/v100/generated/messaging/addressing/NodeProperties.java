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

import com.swiftmq.amqp.v100.generated.transport.definitions.Fields;

import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * </p><p>
 * A symbol-keyed map containing properties of a node used when requesting creation or
 * reporting the creation of a dynamic node.
 * </p><p>
 * </p><p>
 * The following common properties are defined:
 * </p><p>
 * </p><p>
 * lifetime-policy
 * </p><p>
 * </p><p>
 * The lifetime of a dynamically generated node.
 * </p><p>
 * </p><p>
 * Definitionally, the lifetime will never be less than the lifetime of the link which
 * caused its creation, however it is possible to extend the lifetime of dynamically
 * created node using a lifetime policy. The value of this entry must be of a type
 * which provides the  lifetime-policy  archetype. The following standard
 * lifetime-policies  are defined below:  ,
 * ,   or
 * .
 * </p><p>
 * </p><p>
 * supported-dist-modes
 * </p><p>
 * </p><p>
 * The distribution modes that the node supports.
 * </p><p>
 * </p><p>
 * The value of this entry must be one or more symbols which are valid
 * distribution-mode s. That is, the value must be of the same type as would be
 * valid in a field defined with the following attributes:
 * </p><p>
 * </p><p>
 * type="symbol" multiple="true" requires="distribution-mode"
 * </p><p>
 * </p><p>
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class NodeProperties extends Fields {


    /**
     * Constructs a NodeProperties.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public NodeProperties(Map initValue) throws IOException {
        super(initValue);
    }


    public String toString() {
        return "[NodeProperties " + super.toString() + "]";
    }
}
