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
import com.swiftmq.jms.MessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;

public class FileDeleteReply extends MessageBasedReply {
    public FileDeleteReply(Message message) throws JMSException {
        super(message);
    }

    public FileDeleteReply() {
    }

    public Message toMessage() throws JMSException {
        Message message = new MessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEDELETE_REP);
        return fillMessage(message);
    }

    public String toString() {
        return "[FileDeleteReply " + super.toString() + "]";
    }
}
