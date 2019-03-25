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

public class ConnectRequest extends Request {
    int connectId = 0;
    String hostname = null;
    String toolName = null;
    boolean subscribeRouteInfos = false;
    boolean subscribeRouterConfig = false;
    boolean subscribeChangeEvents = false;

    public ConnectRequest(int connectId, String hostname, String toolName, boolean subscribeRouteInfos, boolean subscribeRouterConfig, boolean subscribeChangeEvents) {
        super(0, true);
        this.connectId = connectId;
        this.hostname = hostname;
        this.toolName = toolName;
        this.subscribeRouteInfos = subscribeRouteInfos;
        this.subscribeRouterConfig = subscribeRouterConfig;
        this.subscribeChangeEvents = subscribeChangeEvents;
    }

    public ConnectRequest() {
        this(0, null, null, false, false, false);
    }

    public int getConnectId() {
        return connectId;
    }

    public void setConnectId(int connectId) {
        this.connectId = connectId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public boolean isSubscribeRouteInfos() {
        return subscribeRouteInfos;
    }

    public void setSubscribeRouteInfos(boolean subscribeRouteInfos) {
        this.subscribeRouteInfos = subscribeRouteInfos;
    }

    public boolean isSubscribeRouterConfig() {
        return subscribeRouterConfig;
    }

    public void setSubscribeRouterConfig(boolean subscribeRouterConfig) {
        this.subscribeRouterConfig = subscribeRouterConfig;
    }

    public boolean isSubscribeChangeEvents() {
        return subscribeChangeEvents;
    }

    public void setSubscribeChangeEvents(boolean subscribeChangeEvents) {
        this.subscribeChangeEvents = subscribeChangeEvents;
    }

    public int getDumpId() {
        return ProtocolFactory.CONNECT_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeInt(connectId);
        out.writeUTF(hostname);
        out.writeUTF(toolName);
        out.writeBoolean(subscribeRouteInfos);
        out.writeBoolean(subscribeRouterConfig);
        out.writeBoolean(subscribeChangeEvents);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        connectId = in.readInt();
        hostname = in.readUTF();
        toolName = in.readUTF();
        subscribeRouteInfos = in.readBoolean();
        subscribeRouterConfig = in.readBoolean();
        subscribeChangeEvents = in.readBoolean();
    }

    protected Reply createReplyInstance() {
        return new ConnectReply();
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[ConnectRequest " + super.toString() + ", connectId=" + connectId + ", hostname=" + hostname + ", toolName=" + toolName +
                ", subscribeRouteInfos=" + subscribeRouteInfos + ", subscribeRouterConfig=" + subscribeRouterConfig +
                ", subscribeChangeEvents=" + subscribeChangeEvents + "]";
    }
}
