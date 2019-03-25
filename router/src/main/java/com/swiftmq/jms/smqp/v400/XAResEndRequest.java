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

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class XAResEndRequest extends Request {
    XidImpl xid = null;
    int flags = 0;

    public XAResEndRequest(int dispatchId, XidImpl xid, int flags) {
        super(dispatchId, true);
        this.xid = xid;
        this.flags = flags;
    }

    public int getDumpId() {
        return SMQPFactory.DID_XARESEND_REQ;
    }

    protected Reply createReplyInstance() {
        return new XAResEndReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitXAResEndRequest(this);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        xid.writeContent(out);
        out.writeInt(flags);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        xid = new XidImpl();
        xid.readContent(in);
        flags = in.readInt();
    }

    public XidImpl getXid() {
        return xid;
    }

    public int getFlags() {
        return flags;
    }

    public String toString() {
        return "[XAResEndRequest " + super.toString() + ", xid=" + xid + ", flags=" + flags + "]";
    }

}

