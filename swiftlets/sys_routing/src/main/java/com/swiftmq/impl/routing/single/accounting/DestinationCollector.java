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

package com.swiftmq.impl.routing.single.accounting;

import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DestinationCollector {
    public static final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    public static final String DIRECTION_INBOUND = "INBOUND";
    public static final String DIRECTION_OUTBOUND = "OUTBOUND";
    public static final String DTYPE_QUEUE = "QUEUE";
    public static final String DTYPE_TOPIC = "TOPIC";

    private static final String PROP_SOURCEROUTER = "sourcerouter";
    private static final String PROP_DESTROUTER = "destinationrouter";
    private static final String PROP_DESTNAME = "destinationname";
    private static final String PROP_DESTTYP = "destinationtype";
    private static final String PROP_DIRECTIONTYP = "directiontype";
    private static final String PROP_MESSAGES = "numbermessages";
    private static final String PROP_SIZE = "size";
    private static final String PROP_SIZE_KB = "sizekb";
    private static final String PROP_START_ACCOUNTING = "startaccounting";
    private static final String PROP_END_ACCOUNTING = "endaccounting";

    String key = null;
    String sourceRouter = null;
    String destinationRouter = null;
    String destinationName = null;
    String destinationType = null;
    String directionType = null;
    volatile long totalNumberMsgs = 0;
    volatile long totalSize = 0;
    volatile long txNumberMsgs = 0;
    volatile long txSize = 0;
    volatile long startAccountingTime = -1;
    volatile long endAccountingTime = -1;

    public DestinationCollector(String key, String sourceRouter, String destinationRouter, String destinationName, String destinationType, String directionType) {
        this.key = key;
        this.sourceRouter = sourceRouter;
        this.destinationRouter = destinationRouter;
        this.destinationName = destinationName;
        this.destinationType = destinationType;
        this.directionType = directionType;
        if (destinationType.equals(DTYPE_QUEUE) && destinationName.indexOf('@') == -1)
            this.destinationName = this.destinationName + '@' + SwiftletManager.getInstance().getRouterName();
    }

    void dumpToMapMessage(MapMessageImpl msg) throws Exception {
        msg.setStringProperty(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setStringProperty(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setStringProperty(PROP_SOURCEROUTER, sourceRouter);
        msg.setStringProperty(PROP_DESTROUTER, destinationRouter);
        msg.setStringProperty(PROP_DESTNAME, destinationName);
        msg.setStringProperty(PROP_DESTTYP, destinationType);
        msg.setStringProperty(PROP_DIRECTIONTYP, directionType);
        msg.setString(PROP_START_ACCOUNTING, fmt.format(new Date(startAccountingTime)));
        msg.setString(PROP_END_ACCOUNTING, fmt.format(new Date(endAccountingTime)));
        msg.setString(PROP_SOURCEROUTER, sourceRouter);
        msg.setString(PROP_DESTROUTER, destinationRouter);
        msg.setString(PROP_DESTNAME, destinationName);
        msg.setString(PROP_DESTTYP, destinationType);
        msg.setString(PROP_DIRECTIONTYP, directionType);
        msg.setString(PROP_MESSAGES, new Long(totalNumberMsgs).toString());
        msg.setString(PROP_SIZE, new Long(totalSize).toString());
        msg.setString(PROP_SIZE_KB, new Double((double) totalSize / 1024.0).toString());
    }

    public String getKey() {
        return key;
    }

    public String getDestinationRouter() {
        return destinationRouter;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getDestinationType() {
        return destinationType;
    }

    public String getDirectionType() {
        return directionType;
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

    public void incTx(long nMsgs, long size) {
        if (size <= 0)
            return;
        txNumberMsgs += nMsgs;
        txSize += size;
    }

    public void commit() {
        long time = System.currentTimeMillis();
        if (startAccountingTime == -1)
            startAccountingTime = time;
        endAccountingTime = time;
        totalNumberMsgs += txNumberMsgs;
        totalSize += txSize;
        txNumberMsgs = 0;
        txSize = 0;
    }

    public void abort() {
        txNumberMsgs = 0;
        txSize = 0;
    }

    public void clear() {
        startAccountingTime = -1;
        endAccountingTime = -1;
        totalNumberMsgs = 0;
        totalSize = 0;
    }

    public String toString() {
        return "[DestinationCollector, key=" + key + ", totalNumberMsgs=" + totalNumberMsgs + ", totalSize=" + totalSize + "]";
    }
}
