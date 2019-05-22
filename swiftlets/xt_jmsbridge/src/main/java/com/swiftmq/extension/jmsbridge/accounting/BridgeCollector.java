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

package com.swiftmq.extension.jmsbridge.accounting;

import com.swiftmq.jms.MapMessageImpl;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BridgeCollector {
    public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    public static final String DIRECTION_INBOUND = "INBOUND";
    public static final String DIRECTION_OUTBOUND = "OUTBOUND";
    public static final String DTYPE_QUEUE = "QUEUE";
    public static final String DTYPE_TOPIC = "TOPIC";

    private static final String PROP_SERVERNAME = "servername";
    private static final String PROP_BRIDGENAME = "bridgename";
    private static final String PROP_LOCALNAME = "localname";
    private static final String PROP_LOCALTYPE = "localtype";
    private static final String PROP_REMOTENAME = "remotename";
    private static final String PROP_REMOTETYPE = "remotetype";
    private static final String PROP_DIRECTIONTYP = "directiontype";
    private static final String PROP_MESSAGES = "numbermessages";
    private static final String PROP_SIZE = "size";
    private static final String PROP_SIZE_KB = "sizekb";
    private static final String PROP_START_ACCOUNTING = "startaccounting";
    private static final String PROP_END_ACCOUNTING = "endaccounting";

    String serverName = null;
    String bridgeName = null;
    String localName = null;
    String localType = null;
    String remoteName = null;
    String remoteType = null;
    String directionType = null;
    volatile long totalNumberMsgs = 0;
    volatile long totalSize = 0;
    volatile long txNumberMsgs = 0;
    volatile long txSize = 0;
    volatile long startAccountingTime = -1;
    volatile long endAccountingTime = -1;

    public BridgeCollector(String serverName, String bridgeName, String localName, String localType, String remoteName, String remoteType, String directionType) {
        this.serverName = serverName;
        this.bridgeName = bridgeName;
        this.localName = localName;
        this.localType = localType;
        this.remoteName = remoteName;
        this.remoteType = remoteType;
        this.directionType = directionType;
    }

    public synchronized void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setStringProperty(PROP_SERVERNAME, serverName);
        msg.setStringProperty(PROP_BRIDGENAME, bridgeName);
        msg.setStringProperty(PROP_LOCALNAME, localName);
        msg.setStringProperty(PROP_LOCALTYPE, localType);
        msg.setStringProperty(PROP_REMOTENAME, remoteName);
        msg.setStringProperty(PROP_REMOTETYPE, remoteType);
        msg.setStringProperty(PROP_DIRECTIONTYP, directionType);
        msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setString(PROP_SERVERNAME, serverName);
        msg.setString(PROP_BRIDGENAME, bridgeName);
        msg.setString(PROP_LOCALNAME, localName);
        msg.setString(PROP_LOCALTYPE, localType);
        msg.setString(PROP_REMOTENAME, remoteName);
        msg.setString(PROP_REMOTETYPE, remoteType);
        msg.setString(PROP_DIRECTIONTYP, directionType);
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
        return "[BridgeCollector, serverName=" + serverName + ", bridgeName=" + bridgeName + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
    }
}
