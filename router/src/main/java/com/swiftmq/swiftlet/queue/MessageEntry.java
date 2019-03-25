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

package com.swiftmq.swiftlet.queue;

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.ToClientSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MessageEntry wraps one message entry of a queue, consisting of a message index
 * and the message.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class MessageEntry {
    MessageIndex messageIndex;
    MessageImpl message;
    transient int connectionId = -1;


    /**
     * Create a new MessageEntry.
     */
    public MessageEntry() {
        this(null, null);
    }


    /**
     * Create a new MessageEntry.
     *
     * @param messageIndex message index.
     * @param message      message.
     */
    public MessageEntry(MessageIndex messageIndex, MessageImpl message) {
        this.messageIndex = messageIndex;
        this.message = message;
    }


    /**
     * Sets the message index.
     *
     * @param messageIndex message index.
     */
    public void setMessageIndex(MessageIndex messageIndex) {
        this.messageIndex = messageIndex;
    }


    /**
     * Returns the message index.
     *
     * @return message index.
     */
    public MessageIndex getMessageIndex() {
        return messageIndex;
    }


    /**
     * Sets the message.
     *
     * @param message message.
     */
    public void setMessage(MessageImpl message) {
        this.message = message;
    }


    /**
     * Returns the message.
     *
     * @return message.
     */
    public MessageImpl getMessage() {
        return message;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * Moves message attributes from the message index into the message.
     * This sets JMS properties like delivery count and JMSRedelivered. It will be performed on the
     * client side after receiving the message entry from the router.
     */
    public void moveMessageAttributes() {
        try {
            message.setIntProperty(MessageImpl.PROP_DELIVERY_COUNT, messageIndex.getDeliveryCount());
            message.setJMSRedelivered(messageIndex.getDeliveryCount() > 1);
            message.setMessageIndex(messageIndex);
        } catch (Exception ignored) {
        }
    }


    /**
     * Writes the content of this object to the stream via a to-client serializer (doesn't write routing headers)
     *
     * @param serializer serializer.
     * @throws IOException on error.
     */
    public void writeContent(ToClientSerializer serializer) throws IOException {
        messageIndex.writeContent(serializer.getDataOutput());
        message.writeContent(serializer);
    }

    /**
     * Writes the content of this object to the stream.
     *
     * @param out outstream.
     * @throws IOException on error.
     */
    public void writeContent(DataOutput out) throws IOException {
        messageIndex.writeContent(out);
        message.writeContent(out);
    }


    /**
     * Reads the content from the stream.
     *
     * @param in instream.
     * @throws IOException on error.
     */
    public void readContent(DataInput in) throws IOException {
        messageIndex = new MessageIndex();
        messageIndex.readContent(in);
        message = MessageImpl.createInstance(in.readInt());
        message.readContent(in);
    }

    public String toString() {
        return "[MessageEntry, messageIndex=" + messageIndex + ", message=" + message + ", connectionId=" + connectionId + "]";
    }
}

