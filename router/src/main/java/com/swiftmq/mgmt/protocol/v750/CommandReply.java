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
import com.swiftmq.util.SwiftUtilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommandReply extends Reply {
    String[] result = null;

    public CommandReply(String[] result) {
        this.result = result;
    }

    public CommandReply() {
    }

    public String[] getResult() {
        return result;
    }

    public void setResult(String[] result) {
        this.result = result;
    }

    public int getDumpId() {
        return ProtocolFactory.COMMAND_REP;
    }

    public void writeContent(DataOutput out)
            throws IOException {
        super.writeContent(out);
        if (result != null) {
            out.writeByte(1);
            out.writeInt(result.length);
            for (int i = 0; i < result.length; i++) {
                out.writeUTF(result[i]);
            }
        } else
            out.writeByte(0);
    }

    public void readContent(DataInput in)
            throws IOException {
        super.readContent(in);
        byte set = in.readByte();
        if (set == 1) {
            result = new String[in.readInt()];
            for (int i = 0; i < result.length; i++) {
                result[i] = in.readUTF();
            }
        }
    }

    public String toString() {
        return "[CommandReply " + super.toString() + ", result=" + (result == null ? "null" : SwiftUtilities.concat(result, "\n")) + "]";
    }
}
