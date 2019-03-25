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

public class FileConsumeRequest extends MessageBasedRequest {
    public static final String QUEUENAME_PROP = "JMS_SWIFTMQ_FT_QUEUENAME";
    public static final String REPLYINTERVAL_PROP = "JMS_SWIFTMQ_FT_REPLYINTERVAL";
    public static final String PWDHEXDIGEST_PROP = "JMS_SWIFTMQ_FT_PWDHEXDIGEST";
    String queueName = null;
    String link = null;
    String passwordHexDigest = null;
    int replyInterval = 0;

    public FileConsumeRequest(Message message) throws JMSException {
        super(message);
        queueName = message.getStringProperty(QUEUENAME_PROP);
        replyInterval = message.getIntProperty(REPLYINTERVAL_PROP);
        if (message.propertyExists(PWDHEXDIGEST_PROP))
            passwordHexDigest = message.getStringProperty(PWDHEXDIGEST_PROP);
        link = ((TextMessage) message).getText();
    }

    public FileConsumeRequest(String queueName, String link, String passwordHexDigest, int replyInterval) {
        setReplyRequired(true);
        this.queueName = queueName;
        this.link = link;
        this.passwordHexDigest = passwordHexDigest;
        this.replyInterval = replyInterval;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public int getReplyInterval() {
        return replyInterval;
    }

    public void setReplyInterval(int replyInterval) {
        this.replyInterval = replyInterval;
    }

    public String getPasswordHexDigest() {
        return passwordHexDigest;
    }

    public void setPasswordHexDigest(String passwordHexDigest) {
        this.passwordHexDigest = passwordHexDigest;
    }

    public MessageBasedReply createReplyInstance() {
        return new FileConsumeReply();
    }

    public void accept(MessageBasedRequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public Message toMessage() throws JMSException {
        TextMessage message = new TextMessageImpl();
        fillMessage(message);
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILECONSUME_REQ);
        message.setStringProperty(QUEUENAME_PROP, queueName);
        message.setIntProperty(REPLYINTERVAL_PROP, replyInterval);
        if (passwordHexDigest != null)
            message.setStringProperty(PWDHEXDIGEST_PROP, passwordHexDigest);
        message.setText(link);
        return message;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileConsumeRequest");
        sb.append(", queueName='").append(queueName).append('\'');
        sb.append(", passwordHexDigest='").append(passwordHexDigest).append('\'');
        sb.append(", link='").append(link).append('\'');
        sb.append(", replyInterval='").append(replyInterval).append('\'');
        sb.append(']');
        return sb.toString();
    }
}
