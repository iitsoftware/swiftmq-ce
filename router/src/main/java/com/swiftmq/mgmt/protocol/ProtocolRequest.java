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

package com.swiftmq.mgmt.protocol;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProtocolRequest extends Request {
    int version = 0;

    public ProtocolRequest(int version) {
        super(0, true);
        this.version = version;
    }

    public ProtocolRequest() {
        this(0);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getDumpId() {
        return ProtocolFactory.PROTOCOL_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeInt(version);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        version = in.readInt();
    }

    protected Reply createReplyInstance() {
        return new ProtocolReply();
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[ProtocolRequest " + super.toString() + ", version=" + version + "]";
    }
}
