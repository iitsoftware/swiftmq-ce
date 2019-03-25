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
 * Represents a true or false value
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class AMQPBoolean extends AMQPType {
    public static final AMQPBoolean TRUE = new AMQPBoolean(true);
    public static final AMQPBoolean FALSE = new AMQPBoolean(false);

    /**
     * Constructs an AMQPBoolean withe a code AMQPTypeDecoder.TRUE or AMQPTypeDecoder.FALSE
     *
     * @param code code
     */
    public AMQPBoolean(int code) {
        super("boolean", code);
    }

    /**
     * Constructs an AMQPBoolean withe a boolean
     *
     * @param value value
     */
    public AMQPBoolean(boolean value) {
        super("boolean", value ? AMQPTypeDecoder.TRUE : AMQPTypeDecoder.FALSE);
    }

    /**
     * Returns the value.
     *
     * @return value
     */
    public boolean getValue() {
        return code == AMQPTypeDecoder.TRUE ? true : false;
    }

    public void readContent(DataInput in) throws IOException {
        if (code == AMQPTypeDecoder.BOOLEAN) {
            byte b = in.readByte();
            setCode(b == 0x00 ? AMQPTypeDecoder.FALSE : AMQPTypeDecoder.TRUE);
        }
    }

    public void writeContent(DataOutput out) throws IOException {
        // Only need to write the code
        super.writeContent(out);
    }

    public String getValueString() {
        return code == AMQPTypeDecoder.TRUE ? "TRUE" : "FALSE";
    }

    public String toString() {
        return "[AMQPBoolean, value=" + getValue() + " " + super.toString() + "]";
    }
}
