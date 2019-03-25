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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MessageIndex is the index of one message in a queue.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class MessageIndex implements Comparable {
    long id;
    int priority;
    int deliveryCount;
    int txId = -1;
    int hashCode = 0;

    /**
     * Creates a new MessageIndex.
     */
    public MessageIndex() {
        this(-1, -1, -1);
    }

    /**
     * Creates a new MessageIndex.
     *
     * @param id            message id.
     * @param priority      priority.
     * @param deliveryCount delivery count.
     */
    public MessageIndex(long id, int priority, int deliveryCount) {
        this.priority = priority;
        this.id = id;
        this.deliveryCount = deliveryCount;
        newHashCode();
    }

    private void newHashCode() {
        this.hashCode = (int) (this.id ^ (this.id >>> 32));
    }

    /**
     * Sets the message id.
     *
     * @param id message id.
     */
    public void setId(long id) {
        this.id = id;
        newHashCode();
    }

    /**
     * Returns the message id.
     *
     * @return message id.
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the priority.
     *
     * @param priority priority.
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Returns the priority.
     *
     * @return priority.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Sets the delivery count.
     *
     * @param deliveryCount delivery count.
     */
    public void setDeliveryCount(int deliveryCount) {
        this.deliveryCount = deliveryCount;
    }

    /**
     * Returns the delivery count.
     *
     * @return delivery count.
     */
    public int getDeliveryCount() {
        return deliveryCount;
    }

    /**
     * Returns the transaction id.
     *
     * @return transaction id.
     */
    public int getTxId() {
        return txId;
    }

    /**
     * Sets the transaction id.
     *
     * @param txId transaction id.
     */
    public void setTxId(int txId) {
        this.txId = txId;
    }

    /**
     * Implementation of Comparable.
     *
     * @param obj a message index.
     * @return true/false.
     */
    public boolean equals(Object obj) {
        MessageIndex that = (MessageIndex) obj;
        return that.priority == priority && that.id == id;
    }

    /**
     * Implementation of Comparable.
     *
     * @param obj a message index.
     * @return true/false.
     */
    public int compareTo(Object obj) {
        MessageIndex that = (MessageIndex) obj;
        if (priority < that.priority)
            return -1;
        if (priority == that.priority) {
            if (id < that.id)
                return -1;
            if (id == that.id)
                return 0;
            return 1;
        }
        return 1;
    }

    public int hashCode() {
        return hashCode;
    }

    /**
     * Writes the content of this object to the stream.
     *
     * @param out outstream.
     * @throws IOException on error.
     */
    public void writeContent(DataOutput out) throws IOException {
        out.writeInt(priority);
        out.writeLong(id);
        out.writeInt(deliveryCount);
        out.writeInt(txId);
    }

    /**
     * Reads the content from the stream.
     *
     * @param in instream.
     * @throws IOException on error.
     */
    public void readContent(DataInput in) throws IOException {
        priority = in.readInt();
        id = in.readLong();
        deliveryCount = in.readInt();
        txId = in.readInt();
    }

    public String toString() {
        return priority + "_" + id;
    }
}

