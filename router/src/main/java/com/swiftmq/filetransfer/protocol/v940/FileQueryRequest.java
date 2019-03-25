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

package com.swiftmq.filetransfer.protocol.v940;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.filetransfer.protocol.MessageBasedRequest;
import com.swiftmq.filetransfer.protocol.MessageBasedRequestVisitor;
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class FileQueryRequest extends MessageBasedRequest {
    String selector = null;

    public FileQueryRequest(Message message) throws JMSException {
        super(message);
        selector = ((TextMessage) message).getText();
    }

    public FileQueryRequest(String selector) {
        this.selector = selector;
        setReplyRequired(true);
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public MessageBasedReply createReplyInstance() {
        return new FileQueryReply();
    }

    public void accept(MessageBasedRequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public Message toMessage() throws JMSException {
        TextMessage message = new TextMessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEQUERY_REQ);
        fillMessage(message);
        message.setText(selector);
        return message;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileQueryRequest");
        sb.append(", selector='").append(selector);
        sb.append(']');
        return sb.toString();
    }
}
