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

package com.swiftmq.jms.smqp.v610;

/**
 * SMQP-Protocol Version 610, Class: XAResCommitRequest
 * Automatically generated, don't change!
 * Generation Date: Mon Jul 17 17:50:11 CEST 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class XAResCommitRequest extends Request {
    private XidImpl xid;
    private boolean onePhase;
    private boolean retry;
    private List recoverRequestList;

    public XAResCommitRequest() {
        super(0, true);
    }

    public XAResCommitRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public XAResCommitRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }

    public XAResCommitRequest(int dispatchId, XidImpl xid, boolean onePhase, boolean retry, List recoverRequestList) {
        super(dispatchId, true);
        this.xid = xid;
        this.onePhase = onePhase;
        this.retry = retry;
        this.recoverRequestList = recoverRequestList;
    }

    public XAResCommitRequest(RequestRetryValidator validator, int dispatchId, XidImpl xid, boolean onePhase, boolean retry, List recoverRequestList) {
        super(dispatchId, true, validator);
        this.xid = xid;
        this.onePhase = onePhase;
        this.retry = retry;
        this.recoverRequestList = recoverRequestList;
    }

    public void setXid(XidImpl xid) {
        this.xid = xid;
    }

    public XidImpl getXid() {
        return xid;
    }

    public void setOnePhase(boolean onePhase) {
        this.onePhase = onePhase;
    }

    public boolean isOnePhase() {
        return onePhase;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRecoverRequestList(List recoverRequestList) {
        this.recoverRequestList = recoverRequestList;
    }

    public List getRecoverRequestList() {
        return recoverRequestList;
    }

    public int getDumpId() {
        return SMQPFactory.DID_XARESCOMMIT_REQ;
    }


    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(xid, out);
        SMQPUtil.write(onePhase, out);
        SMQPUtil.write(retry, out);
        if (recoverRequestList != null) {
            out.writeBoolean(true);
            SMQPUtil.writeRequest(recoverRequestList, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        xid = SMQPUtil.read(xid, in);
        onePhase = SMQPUtil.read(onePhase, in);
        retry = SMQPUtil.read(retry, in);
        boolean recoverRequestList_set = in.readBoolean();
        if (recoverRequestList_set)
            recoverRequestList = SMQPUtil.readRequest(recoverRequestList, in);
    }

    protected Reply createReplyInstance() {
        return new XAResCommitReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v610/XAResCommitRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("xid=");
        _b.append(xid);
        _b.append(", ");
        _b.append("onePhase=");
        _b.append(onePhase);
        _b.append(", ");
        _b.append("retry=");
        _b.append(retry);
        _b.append(", ");
        _b.append("recoverRequestList=");
        _b.append(recoverRequestList);
        _b.append("]");
        return _b.toString();
    }
}
