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

package com.swiftmq.mgmt.protocol.v400;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RouterAvailableRequest extends Request {
    String routername = null;

    public RouterAvailableRequest(String routername) {
        super(0, false);
        this.routername = routername;
    }

    public RouterAvailableRequest() {
        this(null);
    }

    public String getRoutername() {
        return routername;
    }

    public void setRoutername(String routername) {
        this.routername = routername;
    }

    public int getDumpId() {
        return ProtocolFactory.ROUTERAVAILABLE_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeUTF(routername);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        routername = in.readUTF();
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[RouterAvailableRequest " + super.toString() + ", routername=" + routername + "]";
    }
}
