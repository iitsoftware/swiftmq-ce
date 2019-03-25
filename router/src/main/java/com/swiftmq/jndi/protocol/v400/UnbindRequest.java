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

package com.swiftmq.jndi.protocol.v400;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnbindRequest implements JNDIRequest {
    String name;

    public UnbindRequest(String name) {
        this.name = name;
    }

    public UnbindRequest() {
    }

    public int getDumpId() {
        return JNDIRequestFactory.UNBIND;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        out.writeUTF(name);
    }

    public void readContent(DataInput in)
            throws IOException {
        name = in.readUTF();
    }

    public String getName() {
        return (name);
    }

    public void accept(JNDIRequestVisitor visitor) {
        visitor.visit(this);
    }


    public String toString() {
        return "[UnbindRequest, name=" + name + "]";
    }
}

