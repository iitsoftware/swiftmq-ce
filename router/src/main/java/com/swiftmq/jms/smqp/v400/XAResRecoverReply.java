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

import javax.transaction.xa.XAResource;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class XAResRecoverReply extends Reply {
    ArrayList xids = null;
    int errorCode = XAResource.XA_OK;

    public int getDumpId() {
        return SMQPFactory.DID_XARESRECOVER_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        if (xids != null) {
            out.writeBoolean(true);
            out.writeInt(xids.size());
            for (int i = 0; i < xids.size(); i++) {
                XidImpl b = (XidImpl) xids.get(i);
                b.writeContent(out);
            }
        } else
            out.writeBoolean(false);
        out.writeInt(errorCode);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        boolean b = in.readBoolean();
        if (b) {
            int size = in.readInt();
            xids = new ArrayList();
            for (int i = 0; i < size; i++) {
                XidImpl xid = new XidImpl();
                xid.readContent(in);
                xids.add(xid);
            }
        } else
            xids = null;
        errorCode = in.readInt();
    }

    public void setXids(ArrayList xids) {
        this.xids = xids;
    }

    public ArrayList getXids() {
        return xids;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String toString() {
        return "[XAResRecoverReply " + super.toString() + " xids=" + xids + " errorCode=" + errorCode + "]";
    }
}
