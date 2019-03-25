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
import com.swiftmq.jms.BytesMessageImpl;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

public class FileChunkRequest extends MessageBasedRequest {
    public static final String CHUNKNO_PROP = "JMS_SWIFTMQ_FT_CHUNKNO";
    public static final String LENGTH_PROP = "JMS_SWIFTMQ_FT_LENGTH";
    public static final String LAST_PROP = "JMS_SWIFTMQ_FT_LAST";
    int chunkNo = 0;
    byte[] chunk = null;
    int len = 0;
    boolean last = false;

    public FileChunkRequest(Message message) throws JMSException {
        super(message);
        chunkNo = message.getIntProperty(CHUNKNO_PROP);
        len = message.getIntProperty(LENGTH_PROP);
        last = message.getBooleanProperty(LAST_PROP);
        chunk = new byte[len];
        ((BytesMessage) message).readBytes(chunk);
    }

    public FileChunkRequest(boolean replyRequired, int chunkNo, boolean last, byte[] chunk, int len) {
        setReplyRequired(replyRequired);
        this.chunkNo = chunkNo;
        this.last = last;
        this.len = len;
        this.chunk = new byte[len];
        System.arraycopy(chunk, 0, this.chunk, 0, len);
    }

    public int getChunkNo() {
        return chunkNo;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public int getLen() {
        return len;
    }

    public boolean isLast() {
        return last;
    }

    public MessageBasedReply createReplyInstance() {
        if (isReplyRequired())
            return new FileChunkReply();
        return null;
    }

    public void accept(MessageBasedRequestVisitor visitor) {
        ((ProtocolVisitor) visitor).visit(this);
    }

    public Message toMessage() throws JMSException {
        BytesMessage message = new BytesMessageImpl();
        fillMessage(message);
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILECHUNK_REQ);
        message.setIntProperty(CHUNKNO_PROP, chunkNo);
        message.setIntProperty(LENGTH_PROP, len);
        message.setBooleanProperty(LAST_PROP, last);
        message.writeBytes(chunk, 0, len);
        return message;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileChunkRequest");
        sb.append(super.toString());
        sb.append(" chunkNo=").append(chunkNo);
        sb.append(", last=").append(last);
        sb.append(", len=").append(len);
        sb.append(']');
        return sb.toString();
    }
}
