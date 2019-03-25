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

package com.swiftmq.jms.primitives;

import com.swiftmq.tools.dump.Dumpable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class _String implements Dumpable, Primitive {
    // This is a workaround. JMS TCK requires that MapMessage can transfer a null String.
    // This should actually be solved by introducing a new primitive NULLSTRING. However,
    // since this base types are not versioned, a customer would be required to upgrade
    // everything at once (router networks and all clients).
    public static final String NULLMARKER = "\u0000";

    String value = null;

    public _String() {
    }

    public _String(String s) {
        this.value = s;
    }

    public Object getObject() {
        if (value == null || value.equals(NULLMARKER))
            return null;
        return value;
    }

    public int getDumpId() {
        return STRING;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeUTF(value == null ? NULLMARKER : value);
    }

    public void readContent(DataInput in)
            throws IOException {
        value = new String(in.readUTF());
    }

    public String toString() {
        if (value == null || value.equals(NULLMARKER))
            return null;
        return value;
    }
}
