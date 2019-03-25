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
 * SMQP-Protocol Version 600, Class: CreateBrowserRequest
 * Automatically generated, don't change!
 * Generation Date: Thu Feb 09 09:59:46 CET 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateBrowserRequest extends Request {
    private QueueImpl queue;
    private String messageSelector;

    public CreateBrowserRequest() {
        super(0, true);
    }

    public CreateBrowserRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public CreateBrowserRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }

    public CreateBrowserRequest(int dispatchId, QueueImpl queue, String messageSelector) {
        super(dispatchId, true);
        this.queue = queue;
        this.messageSelector = messageSelector;
    }

    public CreateBrowserRequest(RequestRetryValidator validator, int dispatchId, QueueImpl queue, String messageSelector) {
        super(dispatchId, true, validator);
        this.queue = queue;
        this.messageSelector = messageSelector;
    }

    public void setQueue(QueueImpl queue) {
        this.queue = queue;
    }

    public QueueImpl getQueue() {
        return queue;
    }

    public void setMessageSelector(String messageSelector) {
        this.messageSelector = messageSelector;
    }

    public String getMessageSelector() {
        return messageSelector;
    }

    public int getDumpId() {
        return SMQPFactory.DID_CREATEBROWSER_REQ;
    }

    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(queue, out);
        if (messageSelector != null) {
            out.writeBoolean(true);
            SMQPUtil.write(messageSelector, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        queue = SMQPUtil.read(queue, in);
        boolean messageSelector_set = in.readBoolean();
        if (messageSelector_set)
            messageSelector = SMQPUtil.read(messageSelector, in);
    }

    protected Reply createReplyInstance() {
        return new CreateBrowserReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v600/CreateBrowserRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queue=");
        _b.append(queue);
        _b.append(", ");
        _b.append("messageSelector=");
        _b.append(messageSelector);
        _b.append("]");
        return _b.toString();
    }
}
