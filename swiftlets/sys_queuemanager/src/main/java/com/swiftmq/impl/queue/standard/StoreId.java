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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.swiftlet.queue.MessageIndex;

public class StoreId extends MessageIndex {
    boolean persistent = false;
    boolean locked = false;
    long expirationTime = 0;
    long msgSize = 0;
    transient Object persistentKey = null;

    public StoreId(long id, int priority, int deliveryCount, boolean persistent, long expirationTime, Object persistentKey) {
        super(id, priority, deliveryCount);
        this.persistent = persistent;
        this.expirationTime = expirationTime;
        this.persistentKey = persistentKey;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isPersistent() {
        return (persistent);
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public boolean isLocked() {
        return (locked);
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getExpirationTime() {
        return (expirationTime);
    }

    public long getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(long msgSize) {
        this.msgSize = msgSize;
    }

    public void setPersistentKey(Object persistentKey) {
        this.persistentKey = persistentKey;
    }

    public Object getPersistentKey() {
        return (persistentKey);
    }

    public String toString() {
        return "[StoreId, " + super.toString() + ", txId=" + getTxId() + ", persistent=" + persistent + ", msgSize=" + msgSize + ", expirationTime=" + expirationTime + ", locked=" + locked + ", persistentKey=" + persistentKey + "]";
    }
}

