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

public abstract class MessageBasedReply implements MessageBased {
    public static final String OK_PROP = "JMS_SWIFTMQ_FT_OK";
    public static final String EXCEPTION_PROP = "JMS_SWIFTMQ_FT_EXCEPTION";

    boolean ok = true;
    String exception = null;

    protected MessageBasedReply() {
    }

    protected MessageBasedReply(Message message) throws JMSException {
        if (message.propertyExists(OK_PROP))
            ok = message.getBooleanProperty(OK_PROP);
        if (!ok)
            exception = message.getStringProperty(EXCEPTION_PROP);
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public Message fillMessage(Message message) throws JMSException {
        message.setBooleanProperty(OK_PROP, ok);
        message.setStringProperty(EXCEPTION_PROP, exception);
        return message;

    }

    public String toString() {
        StringBuffer sb = new StringBuffer("[MessageBasedReply");
        sb.append(", ok=" + ok);
        sb.append(", exception=" + exception);
        sb.append("]");
        return sb.toString();
    }
}
