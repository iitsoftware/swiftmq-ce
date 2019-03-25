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
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NonXATransactionRequest extends Request {
    int sequenceNo = 0;
    List messageList = null;

    NonXATransactionRequest() {
        this(0, null);
    }

    public NonXATransactionRequest(int sequenceNo, List messageList) {
        super(0, false);
        this.sequenceNo = sequenceNo;
        this.messageList = messageList;
    }

    public int getDumpId() {
        return SMQRFactory.NONXA_TRANSACTION_REQ;
    }

    public void writeContent(DataOutput output) throws IOException {
        super.writeContent(output);
        output.writeInt(sequenceNo);
        output.writeInt(messageList.size());
        for (int i = 0; i < messageList.size(); i++) {
            ((MessageImpl) messageList.get(i)).writeContent(output);
        }
    }

    public void readContent(DataInput input) throws IOException {
        super.readContent(input);
        sequenceNo = input.readInt();
        int size = input.readInt();
        messageList = new ArrayList();
        for (int i = 0; i < size; i++) {
            MessageImpl msg = MessageImpl.createInstance(input.readInt());
            msg.readContent(input);
            messageList.add(msg);
        }
    }

    public int getSequenceNo() {
        return sequenceNo;
    }

    public void setSequenceNo(int sequenceNo) {
        this.sequenceNo = sequenceNo;
    }

    public List getMessageList() {
        return messageList;
    }

    public void setMessageList(List messageList) {
        this.messageList = messageList;
    }

    protected Reply createReplyInstance() {
        return null;
    }

    public void accept(RequestVisitor visitor) {
        ((SMQRVisitor) visitor).handleRequest(this);
    }

    public String toString() {
        return "[TransactionRequest " + super.toString() + ", sequenceNo=" + sequenceNo + ", nMessages=" + (messageList != null ? messageList.size() : 0) + "]";
    }
}
