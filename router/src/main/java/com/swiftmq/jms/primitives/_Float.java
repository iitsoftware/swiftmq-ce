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

public class _Float implements Dumpable, Primitive {
    Float value = null;

    public _Float() {
    }

    public _Float(float value) {
        this.value = new Float(value);
    }

    public _Float(String s) {
        this.value = Float.valueOf(s);
    }

    public float floatValue() {
        return value.floatValue();
    }

    public Object getObject() {
        return value;
    }

    public int getDumpId() {
        return FLOAT;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeFloat(value.floatValue());
    }

    public void readContent(DataInput in)
            throws IOException {
        value = new Float(in.readFloat());
    }

    public String toString() {
        return value.toString();
    }
}
