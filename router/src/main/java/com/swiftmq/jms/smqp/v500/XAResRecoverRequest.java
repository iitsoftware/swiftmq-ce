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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class XAResRecoverRequest extends Request {
    int flag;

    public XAResRecoverRequest(int dispatchId, int flag) {
        super(dispatchId, true);
        this.flag = flag;
    }

    public int getDumpId() {
        return SMQPFactory.DID_XARESRECOVER_REQ;
    }

    protected Reply createReplyInstance() {
        return new XAResRecoverReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visitXAResRecoverRequest(this);
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        out.writeInt(flag);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        flag = in.readInt();
    }

    public int getFlag() {
        return flag;
    }

    public String toString() {
        return "[XAResRecoverRequest " + super.toString() + " flag=" + flag + "]";
    }

}



