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

package com.swiftmq.extension.amqpbridge.accounting;

import com.swiftmq.jms.MapMessageImpl;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BridgeCollector {
    public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static final String PROP_BRIDGETYPE = "bridgetype";
    private static final String PROP_BRIDGENAME = "bridgename";
    private static final String PROP_MESSAGES = "numbermessages";
    private static final String PROP_SIZE = "size";
    private static final String PROP_SIZE_KB = "sizekb";
    private static final String PROP_START_ACCOUNTING = "startaccounting";
    private static final String PROP_END_ACCOUNTING = "endaccounting";

    String bridgeType = null;
    String bridgeName = null;
    volatile long totalNumberMsgs = 0;
    volatile long totalSize = 0;
    volatile long txNumberMsgs = 0;
    volatile long txSize = 0;
    volatile long startAccountingTime = -1;
    volatile long endAccountingTime = -1;

    protected BridgeCollector(String bridgeType, String bridgeName) {
        this.bridgeType = bridgeType;
        this.bridgeName = bridgeName;
    }

    public synchronized void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setStringProperty(PROP_BRIDGETYPE, bridgeType);
        msg.setStringProperty(PROP_BRIDGENAME, bridgeName);
        msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setString(PROP_BRIDGETYPE, bridgeType);
        msg.setString(PROP_BRIDGENAME, bridgeName);
        msg.setString(PROP_MESSAGES, new Long(totalNumberMsgs).toString());
        msg.setString(PROP_SIZE, new Long(totalSize).toString());
        msg.setString(PROP_SIZE_KB, new Double((double) totalSize / 1024.0).toString());
        clear();
    }

    public boolean isDirty() {
        return totalNumberMsgs > 0;
    }

    public synchronized void incTotal(long nMsgs, long size) {
        if (size <= 0)
            return;
        long time = System.currentTimeMillis();
        if (startAccountingTime == -1)
            startAccountingTime = time;
        endAccountingTime = time;
        totalNumberMsgs += nMsgs;
        totalSize += size;
    }

    public synchronized void incTx(long nMsgs, long size) {
        if (size <= 0)
            return;
        txNumberMsgs += nMsgs;
        txSize += size;
    }

    public synchronized void commit() {
        long time = System.currentTimeMillis();
        if (startAccountingTime == -1)
            startAccountingTime = time;
        endAccountingTime = time;
        totalNumberMsgs += txNumberMsgs;
        totalSize += txSize;
        txNumberMsgs = 0;
        txSize = 0;
    }

    public synchronized void abort() {
        txNumberMsgs = 0;
        txSize = 0;
    }

    public synchronized void clear() {
        startAccountingTime = -1;
        endAccountingTime = -1;
        totalNumberMsgs = 0;
        totalSize = 0;
    }

    public String toString() {
        return "[BridgeCollector, bridgeType=" + bridgeType + ", bridgeName=" + bridgeName + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
    }
}
