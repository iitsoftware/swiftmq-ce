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

package com.swiftmq.impl.store.standard.index;

public class QueueIndexEntry extends IndexEntry {
    int priority = 0;
    int deliveryCount = 1;
    long expirationTime = -1;

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public void setDeliveryCount(int deliveryCount) {
        this.deliveryCount = deliveryCount;
    }

    public int getDeliveryCount() {
        return deliveryCount;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public int getLength() {
        if (key == null || rootPageNo == -1)
            throw new NullPointerException("key == null || rootPageNo == -1");
        return 33;
    }

    public void writeContent(byte[] b, int offset) {
        int pos = offset;

        b[pos++] = (byte) (valid ? 1 : 0);

        // write priority
        Util.writeInt(priority, b, pos);
        pos += 4;

        // write deliveryCount
        Util.writeInt(deliveryCount, b, pos);
        pos += 4;

        // write expirationTime
        Util.writeLong(expirationTime, b, pos);
        pos += 8;

        // write the key
        long v = ((Long) key).longValue();
        Util.writeLong(v, b, pos);
        pos += 8;

        // write the rootPageNo
        Util.writeInt(rootPageNo, b, pos);
        pos += 4;
    }

    public void readContent(byte[] b, int offset) {
        int pos = offset;

        valid = b[pos++] == 1;

        // read priority
        priority = Util.readInt(b, pos);
        pos += 4;

        // read deliveryCount
        deliveryCount = Util.readInt(b, pos);
        pos += 4;

        // read expirationTime
        expirationTime = Util.readLong(b, pos);
        pos += 8;

        // read the key
        key = Long.valueOf(Util.readLong(b, pos));
        pos += 8;

        // read the rootPageNo
        rootPageNo = Util.readInt(b, pos);
    }

    public String toString() {
        return "[QueueIndexEntry, " + super.toString() + ", priority=" + priority + ", deliveryCount=" + deliveryCount + ", expirationTime=" + expirationTime + "]";
    }
}

