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
import com.swiftmq.util.SwiftUtilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommandRequest extends Request {
    boolean internal = false;
    String[] context = null;
    String[] command = null;

    public CommandRequest(String[] context, String[] command, boolean internal) {
        super(0, true);
        this.context = context;
        this.command = command;
        this.internal = internal;
    }

    public CommandRequest() {
        this(null, null, false);
    }

    public String[] getContext() {
        return context;
    }

    public void setContext(String[] context) {
        this.context = context;
    }

    public String[] getCommand() {
        return command;
    }

    public void setCommand(String[] command) {
        this.command = command;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
    }

    public int getDumpId() {
        return ProtocolFactory.COMMAND_REQ;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        out.writeBoolean(internal);
        if (context != null) {
            out.writeByte(1);
            out.writeInt(context.length);
            for (int i = 0; i < context.length; i++) {
                out.writeUTF(context[i]);
            }
        } else
            out.writeByte(0);
        out.writeInt(command.length);
        for (int i = 0; i < command.length; i++) {
            out.writeUTF(command[i]);
        }
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        internal = in.readBoolean();
        byte set = in.readByte();
        if (set == 1) {
            context = new String[in.readInt()];
            for (int i = 0; i < context.length; i++) {
                context[i] = in.readUTF();
            }
        }
        command = new String[in.readInt()];
        for (int i = 0; i < command.length; i++) {
            command[i] = in.readUTF();
        }
    }

    protected Reply createReplyInstance() {
        return new CommandReply();
    }

    public void accept(RequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public String toString() {
        return "[CommandRequest " + super.toString() + ", context=" + (context != null ? SwiftUtilities.concat(context, "/") : "null") +
                ", command=" + SwiftUtilities.concat(command, " ") + ", internal=" + internal + "]";
    }
}
