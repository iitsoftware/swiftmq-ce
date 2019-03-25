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

public class DisconnectedRequest extends Request {
    String routerName = null;
    String reason = null;

    public DisconnectedRequest(String routerName, String reason) {
        super(0, false);
        this.routerName = routerName;
        this.reason = reason;
    }

    public DisconnectedRequest() {
        this(null, null);
    }

    public String getRouterName() {
        return routerName;
    }

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public int getDumpId() {
        return ProtocolFactory.DISCONNECTED_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeUTF(routerName);
        out.writeUTF(reason);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        routerName = in.readUTF();
        reason = in.readUTF();
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[DisconnectedRequest " + super.toString() + ", routerName=" + routerName + ", reason=" + reason + "]";
    }
}
