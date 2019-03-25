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

package com.swiftmq.impl.amqp.accounting;

import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DestinationCollector {
    public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    public static final String ATYPE_PRODUCER = "PRODUCER";
    public static final String ATYPE_CONSUMER = "CONSUMER";
    public static final String DTYPE_QUEUE = "QUEUE";
    public static final String DTYPE_TOPIC = "TOPIC";

    private static final String PROP_DESTNAME = "destinationname";
    private static final String PROP_DESTTYP = "destinationtype";
    private static final String PROP_ACCTYP = "accountingtype";
    private static final String PROP_MESSAGES = "numbermessages";
    private static final String PROP_SIZE = "size";
    private static final String PROP_SIZE_KB = "sizekb";
    private static final String PROP_START_ACCOUNTING = "startaccounting";
    private static final String PROP_END_ACCOUNTING = "endaccounting";

    String key = null;
    String destinationName = null;
    String destinationType = null;
    String accountingType = null;
    long totalNumberMsgs = 0;
    long totalSize = 0;
    long startAccountingTime = -1;
    long endAccountingTime = -1;
    Map txMap = new HashMap();

    public DestinationCollector(String key, String destinationName, String destinationType, String accountingType) {
        this.key = key;
        this.destinationName = destinationName;
        this.destinationType = destinationType;
        this.accountingType = accountingType;
        if (destinationType.equals(DTYPE_QUEUE) && destinationName.indexOf('@') == -1)
            this.destinationName = this.destinationName + '@' + SwiftletManager.getInstance().getRouterName();
    }

    void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setStringProperty(PROP_DESTNAME, destinationName);
        msg.setStringProperty(PROP_DESTTYP, destinationType);
        msg.setStringProperty(PROP_ACCTYP, accountingType);
        msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setString(PROP_DESTNAME, destinationName);
        msg.setString(PROP_DESTTYP, destinationType);
        msg.setString(PROP_ACCTYP, accountingType);
        msg.setString(PROP_MESSAGES, new Long(totalNumberMsgs).toString());
        msg.setString(PROP_SIZE, new Long(totalSize).toString());
        msg.setString(PROP_SIZE_KB, new Double((double) totalSize / 1024.0).toString());
    }

    public String getKey() {
        return key;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getDestinationType() {
        return destinationType;
    }

    public String getAccountingType() {
        return accountingType;
    }

    public long getTotalNumberMsgs() {
        return totalNumberMsgs;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getStartAccountingTime() {
        return startAccountingTime;
    }

    public long getEndAccountingTime() {
        return endAccountingTime;
    }

    public boolean isDirty() {
        return totalNumberMsgs > 0;
    }

    public void incTotal(long nMsgs, long size) {
        if (size <= 0)
            return;
        long time = System.currentTimeMillis();
        if (startAccountingTime == -1)
            startAccountingTime = time;
        endAccountingTime = time;
        totalNumberMsgs += nMsgs;
        totalSize += size;
    }

    public void incTx(String txId, long nMsgs, long size) {
        TxEntry txEntry = (TxEntry) txMap.get(txId);
        if (txEntry == null) {
            txEntry = new TxEntry();
            txMap.put(txId, txEntry);
        }
        txEntry.numberMsgs += nMsgs;
        txEntry.size += size;
    }

    public void commit(String txId) {
        long time = System.currentTimeMillis();
        if (startAccountingTime == -1)
            startAccountingTime = time;
        endAccountingTime = time;
        TxEntry txEntry = (TxEntry) txMap.remove(txId);
        if (txEntry != null) {
            totalNumberMsgs += txEntry.numberMsgs;
            totalSize += txEntry.size;
        }
    }

    public void abort(String txId) {
        txMap.remove(txId);
    }

    public void clear() {
        startAccountingTime = -1;
        endAccountingTime = -1;
        totalNumberMsgs = 0;
        totalSize = 0;
        txMap.clear();
    }

    public String toString() {
        return "[DestinationCollector, key=" + key + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
    }

    private class TxEntry {
        long numberMsgs = 0;
        long size = 0;
    }
}
