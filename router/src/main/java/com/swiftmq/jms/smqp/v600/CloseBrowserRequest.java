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

package com.swiftmq.jms.smqp.v600;

/**
 * SMQP-Protocol Version 600, Class: CloseBrowserRequest
 * Automatically generated, don't change!
 * Generation Date: Thu Feb 09 09:59:46 CET 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CloseBrowserRequest extends Request {
    private int queueBrowserId;

    public CloseBrowserRequest() {
        super(0, true);
    }

    public CloseBrowserRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public CloseBrowserRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }

    public CloseBrowserRequest(int dispatchId, int queueBrowserId) {
        super(dispatchId, true);
        this.queueBrowserId = queueBrowserId;
    }

    public CloseBrowserRequest(RequestRetryValidator validator, int dispatchId, int queueBrowserId) {
        super(dispatchId, true, validator);
        this.queueBrowserId = queueBrowserId;
    }

    public void setQueueBrowserId(int queueBrowserId) {
        this.queueBrowserId = queueBrowserId;
    }

    public int getQueueBrowserId() {
        return queueBrowserId;
    }

    public int getDumpId() {
        return SMQPFactory.DID_CLOSEBROWSER_REQ;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(queueBrowserId, out);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        queueBrowserId = SMQPUtil.read(queueBrowserId, in);
    }

    protected Reply createReplyInstance() {
        return new CloseBrowserReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v600/CloseBrowserRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queueBrowserId=");
        _b.append(queueBrowserId);
        _b.append("]");
        return _b.toString();
    }
}
