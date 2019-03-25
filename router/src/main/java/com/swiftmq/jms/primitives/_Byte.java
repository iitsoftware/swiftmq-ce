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

public class _Byte implements Dumpable, Primitive {
    Byte value = null;

    public _Byte() {
    }

    public _Byte(byte value) {
        this.value = new Byte(value);
    }

    public _Byte(String s) {
        this.value = Byte.valueOf(s);
    }

    public byte byteValue() {
        return value.byteValue();
    }

    public Object getObject() {
        return value;
    }

    public int getDumpId() {
        return BYTE;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeByte(value.byteValue());
    }

    public void readContent(DataInput in)
            throws IOException {
        value = new Byte(in.readByte());
    }

    public String toString() {
        return value.toString();
    }
}
