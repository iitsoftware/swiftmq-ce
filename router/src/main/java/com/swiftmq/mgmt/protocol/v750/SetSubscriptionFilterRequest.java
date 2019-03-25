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
import com.swiftmq.util.SwiftUtilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetSubscriptionFilterRequest extends Request {
    String[] context = null;
    boolean includeNextLevel = false;

    SetSubscriptionFilterRequest() {
        this(null, false);
    }

    public SetSubscriptionFilterRequest(String[] context, boolean includeNextLevel) {
        super(0, false);
        this.context = context;
        this.includeNextLevel = includeNextLevel;
    }

    public int getDumpId() {
        return ProtocolFactory.SETSUBSCRIPTIONFILTER_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        output.writeBoolean(includeNextLevel);
        output.writeInt(context.length);
        for (int i = 0; i < context.length; i++)
            output.writeUTF(context[i]);
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        includeNextLevel = input.readBoolean();
        int len = input.readInt();
        context = new String[len];
        for (int i = 0; i < len; i++)
            context[i] = input.readUTF();
    }

    public String[] getContext() {
        return context;
    }

    public void setContext(String[] context) {
        this.context = context;
    }

    public boolean isIncludeNextLevel() {
        return includeNextLevel;
    }

    public void setIncludeNextLevel(boolean includeNextLevel) {
        this.includeNextLevel = includeNextLevel;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[SetSubscriptionFilterRequest " + super.toString() + ", context=" + SwiftUtilities.concat(context, "/") + ", includeNextLevel=" + includeNextLevel + "]";
    }
}