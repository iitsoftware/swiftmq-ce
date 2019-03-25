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

package com.swiftmq.jms.smqp.v510;

/**
 * SMQP-Protocol Version 510, Class: XAResRecoverReply
 * Automatically generated, don't change!
 * Generation Date: Fri Aug 13 16:00:44 CEST 2004
 * (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class XAResRecoverReply extends ReplyNE {
    private int errorCode;
    private List xids;

    public XAResRecoverReply(int errorCode, List xids) {
        this.errorCode = errorCode;
        this.xids = xids;
    }

    protected XAResRecoverReply() {
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setXids(List xids) {
        this.xids = xids;
    }

    public List getXids() {
        return xids;
    }

    public int getDumpId() {
        return SMQPFactory.DID_XARESRECOVER_REP;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(errorCode, out);
        if (xids != null) {
            out.writeBoolean(true);
            SMQPUtil.writeXid(xids, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        errorCode = SMQPUtil.read(errorCode, in);
        boolean xids_set = in.readBoolean();
        if (xids_set)
            xids = SMQPUtil.readXid(xids, in);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[XAResRecoverReply, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("errorCode=");
        _b.append(errorCode);
        _b.append(", ");
        _b.append("xids=");
        _b.append(xids);
        _b.append("]");
        return _b.toString();
    }
}
