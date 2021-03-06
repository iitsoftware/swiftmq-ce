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

package com.swiftmq.impl.routing.single.smqpr.v400;

import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConnectRequest extends Request {
    String routerName = null;

    ConnectRequest() {
        this(null);
    }

    public ConnectRequest(String routerName) {
        super(0, false);
        this.routerName = routerName;
    }

    public int getDumpId() {
        return SMQRFactory.CONNECT_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        output.writeUTF(routerName);
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        routerName = input.readUTF();
    }

    public String getRouterName() {
        return routerName;
    }

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQRVisitor) visitor).handleRequest(this);
    }

    public String toString() {
        return "[ConnectRequest " + super.toString() + ", routerName=" + routerName + "]";
    }
}
