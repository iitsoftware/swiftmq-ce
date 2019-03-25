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

public interface Primitive {
    public static final int INT = 0;
    public static final int LONG = 1;
    public static final int DOUBLE = 2;
    public static final int FLOAT = 3;
    public static final int BOOLEAN = 4;
    public static final int CHAR = 5;
    public static final int SHORT = 6;
    public static final int BYTE = 7;
    public static final int BYTES = 8;
    public static final int STRING = 9;

    public Object getObject();
}
