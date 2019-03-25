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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProtocolRequest extends Request {
    List protocolVersions = null;

    ProtocolRequest() {
        this(null);
    }

    public ProtocolRequest(List protocolVersions) {
        super(0, false);
        this.protocolVersions = protocolVersions;
    }

    public int getDumpId() {
        return SMQRFactory.PROTOCOL_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        output.writeInt(protocolVersions.size());
        for (int i = 0; i < protocolVersions.size(); i++)
            output.writeUTF((String) protocolVersions.get(i));
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        protocolVersions = new ArrayList();
        int size = input.readInt();
        for (int i = 0; i < size; i++)
            protocolVersions.add(input.readUTF());
    }

    public List getProtocolVersions() {
        return protocolVersions;
    }

    public void setProtocolVersions(List protocolVersions) {
        this.protocolVersions = protocolVersions;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQRVisitor) visitor).handleRequest(this);
    }

    public String toString() {
        return "[ProtocolRequest " + super.toString() + ", protocolVersions=" + protocolVersions + "]";
    }
}
