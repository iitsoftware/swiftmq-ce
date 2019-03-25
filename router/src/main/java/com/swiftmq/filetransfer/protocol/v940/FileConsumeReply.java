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

public class FileConsumeReply extends MessageBasedReply {
    public static final String FILENAME_PROP = "JMS_SWIFTMQ_FT_FILENAME";
    public static final String SIZE_PROP = "JMS_SWIFTMQ_FT_SIZE";
    public static final String CHUNKLENGTH_PROP = "JMS_SWIFTMQ_FT_CHUNKLENGTH";
    String filename = null;
    int chunkLength = 0;
    long size = 0;

    public FileConsumeReply(Message message) throws JMSException {
        super(message);
        filename = message.getStringProperty(FILENAME_PROP);
        chunkLength = message.getIntProperty(CHUNKLENGTH_PROP);
        size = message.getLongProperty(SIZE_PROP);
    }

    public FileConsumeReply(String filename, int chunkLength, long size) {
        this.filename = filename;
        this.size = size;
    }

    public FileConsumeReply() {
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public int getChunkLength() {
        return chunkLength;
    }

    public void setChunkLength(int chunkLength) {
        this.chunkLength = chunkLength;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Message toMessage() throws JMSException {
        Message message = new MessageImpl();
        message.setIntProperty(ProtocolFactory.DUMPID_PROP, ProtocolFactory.FILECONSUME_REP);
        message.setStringProperty(FILENAME_PROP, filename);
        message.setIntProperty(CHUNKLENGTH_PROP, chunkLength);
        message.setLongProperty(SIZE_PROP, size);
        return fillMessage(message);

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[FileConsumeReply");
        sb.append(super.toString());
        sb.append(", filename='").append(filename).append('\'');
        sb.append(", chunkLength=").append(chunkLength);
        sb.append(", size=").append(size);
        sb.append(']');
        return sb.toString();
    }
}
