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

package com.swiftmq.filetransfer.protocol.v941;

import com.swiftmq.filetransfer.protocol.MessageBasedReply;
import com.swiftmq.filetransfer.protocol.MessageBasedRequest;
import com.swiftmq.filetransfer.protocol.MessageBasedRequestVisitor;
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class FileQueryPropsRequest extends MessageBasedRequest {
    public static final String LINK_PROP = "JMS_SWIFTMQ_FT_LINK";
    String link = null;
    String selector = null;

    public FileQueryPropsRequest(Message message) throws JMSException {
        super(message);
        link = message.getStringProperty(LINK_PROP);
        selector = ((TextMessage) message).getText();
    }

    public FileQueryPropsRequest(String link, String selector) {
        this.link = link;
        this.selector = selector;
        setReplyRequired(true);
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public MessageBasedReply createReplyInstance() {
        return new FileQueryPropsReply();
    }

    public void accept(MessageBasedRequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public Message toMessage() throws JMSException {
        TextMessage message = new TextMessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEQUERYPROPS_REQ);
        if (link != null)
            message.setStringProperty(LINK_PROP, link);
        fillMessage(message);
        message.setText(selector);
        return message;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileQueryPropsRequest");
        sb.append(", link='").append(link);
        sb.append(", selector='").append(selector);
        sb.append(']');
        return sb.toString();
    }
}
