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
 * SMQP-Protocol Version 610, Class: AcknowledgeMessageRequest
 * Automatically generated, don't change!
 * Generation Date: Mon Jul 17 17:50:10 CEST 2006
 * (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AcknowledgeMessageRequest extends Request {
    private int queueConsumerId;
    private MessageIndex messageIndex;

    public AcknowledgeMessageRequest() {
        super(0, true);
    }

    public AcknowledgeMessageRequest(int dispatchId) {
        super(dispatchId, true);
    }

    public AcknowledgeMessageRequest(RequestRetryValidator validator, int dispatchId) {
        super(dispatchId, true, validator);
    }

    public AcknowledgeMessageRequest(int dispatchId, int queueConsumerId, MessageIndex messageIndex) {
        super(dispatchId, true);
        this.queueConsumerId = queueConsumerId;
        this.messageIndex = messageIndex;
    }

    public AcknowledgeMessageRequest(RequestRetryValidator validator, int dispatchId, int queueConsumerId, MessageIndex messageIndex) {
        super(dispatchId, true, validator);
        this.queueConsumerId = queueConsumerId;
        this.messageIndex = messageIndex;
    }

    public void setQueueConsumerId(int queueConsumerId) {
        this.queueConsumerId = queueConsumerId;
    }

    public int getQueueConsumerId() {
        return queueConsumerId;
    }

    public void setMessageIndex(MessageIndex messageIndex) {
        this.messageIndex = messageIndex;
    }

    public MessageIndex getMessageIndex() {
        return messageIndex;
    }

    public int getDumpId() {
        return SMQPFactory.DID_ACKNOWLEDGEMESSAGE_REQ;
    }


    public void writeContent(DataOutput out) throws IOException {
        super.writeContent(out);
        SMQPUtil.write(queueConsumerId, out);
        if (messageIndex != null) {
            out.writeBoolean(true);
            SMQPUtil.write(messageIndex, out);
        } else
            out.writeBoolean(false);
    }

    public void readContent(DataInput in) throws IOException {
        super.readContent(in);
        queueConsumerId = SMQPUtil.read(queueConsumerId, in);
        boolean messageIndex_set = in.readBoolean();
        if (messageIndex_set)
            messageIndex = SMQPUtil.read(messageIndex, in);
    }

    protected Reply createReplyInstance() {
        return new AcknowledgeMessageReply();
    }

    public void accept(RequestVisitor visitor) {
        ((SMQPVisitor) visitor).visit(this);
    }

    public String toString() {
        StringBuffer _b = new StringBuffer("[v610/AcknowledgeMessageRequest, ");
        _b.append(super.toString());
        _b.append(", ");
        _b.append("queueConsumerId=");
        _b.append(queueConsumerId);
        _b.append(", ");
        _b.append("messageIndex=");
        _b.append(messageIndex);
        _b.append("]");
        return _b.toString();
    }
}
