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

package com.swiftmq.impl.routing.single.smqpr.v942;

import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.ReplyRequest;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryReplyRequest extends ReplyRequest {
    List xidList = null;

    public RecoveryReplyRequest() {
        super(0, false);
    }

    public int getDumpId() {
        return SMQRFactory.RECOVERY_REPREQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        if (xidList == null)
            output.writeInt(0);
        else {
            output.writeInt(xidList.size());
            for (int i = 0; i < xidList.size(); i++) {
                ((XidImpl) xidList.get(i)).writeContent(output);
            }
        }
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        int size = input.readInt();
        if (size > 0) {
            xidList = new ArrayList();
            for (int i = 0; i < size; i++) {
                XidImpl xid = new XidImpl();
                xid.readContent(input);
                xidList.add(xid);
            }
        }
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQRVisitor) visitor).handleRequest(this);
    }

    public List getXidList() {
        return xidList;
    }

    public void setXidList(List xidList) {
        this.xidList = xidList;
    }

    public String toString() {
        return "[RecoveryReplyRequest " + super.toString() + ", xidList=" + xidList + "]";
    }
}
