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

package com.swiftmq.filetransfer.protocol;

import javax.jms.JMSException;
import javax.jms.Message;

public abstract class MessageBasedRequest implements MessageBased {
    public static final String REPLYREQ_PROP = "JMS_SWIFTMQ_FT_REPLYREQUIRED";

    boolean replyRequired = false;
    transient Message message = null;

    protected MessageBasedRequest() {
    }

    protected MessageBasedRequest(Message message) throws JMSException {
        if (message.propertyExists(REPLYREQ_PROP))
            replyRequired = message.getBooleanProperty(REPLYREQ_PROP);
    }

    public boolean isReplyRequired() {
        return replyRequired;
    }

    public void setReplyRequired(boolean replyRequired) {
        this.replyRequired = replyRequired;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public Message fillMessage(Message message) throws JMSException {
        message.setBooleanProperty(REPLYREQ_PROP, replyRequired);
        return message;

    }

    public abstract MessageBasedReply createReplyInstance();

    public abstract void accept(MessageBasedRequestVisitor visitor);

    public String toString() {
        return new StringBuffer(" replyRequired=").append(replyRequired).toString();
    }
}
