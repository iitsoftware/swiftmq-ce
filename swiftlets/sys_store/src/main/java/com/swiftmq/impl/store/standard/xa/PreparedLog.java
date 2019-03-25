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

import com.swiftmq.jms.XidImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class PreparedLog {
    public abstract long add(PrepareLogRecordImpl logRecord) throws IOException;

    public abstract PrepareLogRecordImpl get(long address) throws IOException;

    public abstract List getAll() throws IOException;

    public abstract void remove(PrepareLogRecordImpl logRecord) throws IOException;

    public List getPreparedXids() throws IOException {
        List lrList = getAll();
        List xidList = new ArrayList();
        for (int i = 0; i < lrList.size(); i++) {
            PrepareLogRecordImpl rec = (PrepareLogRecordImpl) lrList.get(i);
            XidImpl xid = rec.getGlobalTxId();
            boolean isNew = true;
            for (int j = 0; j < xidList.size(); j++) {
                if (xid.equals(xidList.get(j))) {
                    isNew = false;
                    break;
                }
            }
            if (isNew)
                xidList.add(xid);
        }
        return xidList;
    }

    public List getQueuesForXid(byte[] xid) throws IOException {
        List lrList = getAll();
        List queueList = new ArrayList();
        for (int i = 0; i < lrList.size(); i++) {
            PrepareLogRecordImpl rec = (PrepareLogRecordImpl) lrList.get(i);
            if (xid.equals(rec.getGlobalTxId())) {
                String queueName = rec.getQueueName();
                boolean isNew = true;
                for (int j = 0; j < queueList.size(); j++) {
                    if (queueName.equals(queueList.get(j))) {
                        isNew = false;
                        break;
                    }
                }
                if (isNew)
                    queueList.add(queueName);
            }
        }
        return queueList;
    }

    public abstract boolean backupRequired();

    public abstract void backup(String destPath) throws Exception;
}
