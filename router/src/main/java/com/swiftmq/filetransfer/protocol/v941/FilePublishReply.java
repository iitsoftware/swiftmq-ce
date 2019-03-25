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
import com.swiftmq.jms.MessageImpl;

import javax.jms.JMSException;
import javax.jms.Message;

public class FilePublishReply extends MessageBasedReply {
    public static final String CHUNKLENGTH_PROP = "JMS_SWIFTMQ_FT_CHUNKLENGTH";
    int chunkLength = 0;

    public FilePublishReply(Message message) throws JMSException {
        super(message);
        chunkLength = message.getIntProperty(CHUNKLENGTH_PROP);
    }

    public FilePublishReply(int chunkLength) {
        this.chunkLength = chunkLength;
    }

    public FilePublishReply() {
        this(0);
    }

    public int getChunkLength() {
        return chunkLength;
    }

    public void setChunkLength(int chunkLength) {
        this.chunkLength = chunkLength;
    }

    public Message toMessage() throws JMSException {
        Message message = new MessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILEPUBLISH_REP);
        message.setIntProperty(CHUNKLENGTH_PROP, chunkLength);
        return fillMessage(message);

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FilePublishReply");
        sb.append(super.toString());
        sb.append(", replyInterval=").append(chunkLength);
        sb.append(']');
        return sb.toString();
    }
}
