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

package com.swiftmq.impl.store.standard.xa;

import com.swiftmq.impl.store.standard.index.QueueIndexEntry;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.store.PrepareLogRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PrepareLogRecordImpl implements PrepareLogRecord {
    boolean valid = true;
    long preparationTime = 0;
    int type;
    String queueName = null;
    XidImpl globalTxId = null;
    List keyList = null;
    byte[] buffer = null;
    long address = -1;

    public PrepareLogRecordImpl(long address) {
        this.address = address;
    }

    public PrepareLogRecordImpl(int type, String queueName, XidImpl globalTxId, List keys) {
        this.type = type;
        this.queueName = queueName;
        this.globalTxId = globalTxId;
        keyList = new ArrayList();
        keyList.addAll(keys);
        preparationTime = System.currentTimeMillis();
    }

    public void setAddress(long address) {
        this.address = address;
    }

    public long getAddress() {
        return address;
    }

    void setValid(boolean b) {
        this.valid = b;
    }

    public boolean isValid() {
        return valid;
    }

    public long getPreparationTime() {
        return preparationTime;
    }

    public int getType() {
        return type;
    }

    public String getQueueName() {
        return queueName;
    }

    public XidImpl getGlobalTxId() {
        return globalTxId;
    }

    public List getKeyList() {
        return keyList;
    }

    private void writeEntry(DataOutput out, QueueIndexEntry entry) throws IOException {
        if (buffer == null || buffer.length < entry.getLength())
            buffer = new byte[entry.getLength()];
        entry.writeContent(buffer, 0);
        out.writeInt(entry.getLength());
        out.write(buffer, 0, entry.getLength());
    }

    private QueueIndexEntry readEntry(DataInput in) throws IOException {
        int len = in.readInt();
        if (buffer == null || buffer.length < len)
            buffer = new byte[len];
        in.readFully(buffer, 0, len);
        QueueIndexEntry entry = new QueueIndexEntry();
        entry.readContent(buffer, 0);
        return entry;
    }

    void writeContent(DataOutput out) throws IOException {
        out.writeBoolean(valid);
        out.writeLong(preparationTime);
        out.writeInt(type);
        out.writeUTF(queueName);
        globalTxId.writeContent(out);
        out.writeInt(keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            writeEntry(out, (QueueIndexEntry) keyList.get(i));
        }
    }

    void writeValid(DataOutput out) throws IOException {
        out.writeBoolean(valid);
    }

    void readContent(DataInput in) throws IOException {
        valid = in.readBoolean();
        preparationTime = in.readLong();
        type = in.readInt();
        queueName = in.readUTF();
        globalTxId = new XidImpl();
        globalTxId.readContent(in);
        int size = in.readInt();
        keyList = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            keyList.add(readEntry(in));
        }
    }

    public String toString() {
        return "[PrepareLogRecord, address=" + address + ", valid=" + valid + ", type=" + (type == 0 ? "READ_TRANSACTION" : "WRITE_TRANSACTION") +
                ", preparationTime=" + new Date(preparationTime).toString() + ", queueName=" + queueName +
                ", globalTxId=" + globalTxId + ", keys=" + keyList;
    }
}
