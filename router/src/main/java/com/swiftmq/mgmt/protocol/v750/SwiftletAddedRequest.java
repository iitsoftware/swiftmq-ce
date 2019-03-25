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

package com.swiftmq.mgmt.protocol.v750;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SwiftletAddedRequest extends Request {
    String name = null;
    byte[] buffer = null;
    int length = 0;

    public SwiftletAddedRequest(String name, byte[] buffer, int length) {
        super(0, false);
        this.name = name;
        this.buffer = new byte[length];
        this.length = length;
        System.arraycopy(buffer, 0, this.buffer, 0, length);
    }

    public SwiftletAddedRequest() {
        super(0, false);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = new byte[buffer.length];
        System.arraycopy(buffer, 0, this.buffer, 0, buffer.length);
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getDumpId() {
        return ProtocolFactory.SWIFTLETADDED_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeUTF(name);
        out.writeInt(length);
        out.write(buffer, 0, length);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        name = in.readUTF();
        length = in.readInt();
        buffer = new byte[length];
        in.readFully(buffer);
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[SwiftletAddedRequest " + super.toString() + ", name=" + name + ", length=" + length + "]";
    }
}
