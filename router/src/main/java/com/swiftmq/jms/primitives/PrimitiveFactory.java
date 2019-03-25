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

public class PrimitiveFactory {
    public static Primitive createInstance(int id) {
        Primitive d = null;
        switch (id) {
            case Primitive.INT:
                d = new _Int();
                break;
            case Primitive.LONG:
                d = new _Long();
                break;
            case Primitive.DOUBLE:
                d = new _Double();
                break;
            case Primitive.FLOAT:
                d = new _Float();
                break;
            case Primitive.BOOLEAN:
                d = new _Boolean();
                break;
            case Primitive.CHAR:
                d = new _Char();
                break;
            case Primitive.SHORT:
                d = new _Short();
                break;
            case Primitive.BYTE:
                d = new _Byte();
                break;
            case Primitive.BYTES:
                d = new _Bytes();
                break;
            case Primitive.STRING:
                d = new _String();
                break;
            default:
                throw new RuntimeException("PrimitiveFactory, can't create dumpable from dumpId: " + id);
        }
        return d;
    }
}
