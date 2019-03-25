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

package com.swiftmq.amqp.v100.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Indicates an empty value
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPNull extends AMQPType {
    public static final AMQPNull NULL = new AMQPNull();

    /**
     * Constructs an AMQPNull
     */
    public AMQPNull() {
        super("null", AMQPTypeDecoder.NULL);
    }

    public void readContent(DataInput in) throws IOException {
        // Do nothing as the value is encoded in the code
    }

    public void writeContent(DataOutput out) throws IOException {
        // Only need to write the code
        super.writeContent(out);
    }

    public String getValueString() {
        return "NULL";
    }

    public String toString() {
        return "[AMQPNull " + super.toString() + "]";
    }
}
