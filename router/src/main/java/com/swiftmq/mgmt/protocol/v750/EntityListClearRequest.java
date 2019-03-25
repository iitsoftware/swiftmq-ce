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

public class EntityListClearRequest extends Request {
    String[] context = null;

    public EntityListClearRequest(String[] context) {
        super(0, false);
        this.context = context;
    }

    public EntityListClearRequest() {
        this(null);
    }

    public String[] getContext() {
        return context;
    }

    public void setContext(String[] context) {
        this.context = context;
    }

    public int getDumpId() {
        return ProtocolFactory.ENTITYLISTCLEAR_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        if (context != null) {
            out.writeByte(1);
            out.writeInt(context.length);
            for (int i = 0; i < context.length; i++) {
                out.writeUTF(context[i]);
            }
        } else
            out.writeByte(0);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        byte set = in.readByte();
        if (set == 1) {
            context = new String[in.readInt()];
            for (int i = 0; i < context.length; i++) {
                context[i] = in.readUTF();
            }
        }
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[EntityListClearRequest " + super.toString() + ", context=" + (context != null ? SwiftUtilities.concat(context, "/") : "null") + "]";
    }
}