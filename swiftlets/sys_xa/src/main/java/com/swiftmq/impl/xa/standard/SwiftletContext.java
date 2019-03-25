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

package com.swiftmq.impl.xa.standard;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext {
    public LogSwiftlet logSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public StoreSwiftlet storeSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public QueueManager queueManager = null;
    public Configuration config = null;
    public EntityList preparedUsageList = null;
    public EntityList heuristicUsageList = null;
    public XAResourceManagerSwiftletImpl xaSwiftlet = null;
    public HeuristicHandler heuristicHandler = null;

    public SwiftletContext(XAResourceManagerSwiftletImpl xaSwiftlet, Configuration config) {
        this.xaSwiftlet = xaSwiftlet;
        this.config = config;

        preparedUsageList = (EntityList) config.getEntity("usage").getEntity("prepared-tx");
        heuristicUsageList = (EntityList) config.getEntity("usage").getEntity("heuristic-tx");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        storeSwiftlet = (StoreSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$store");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        heuristicHandler = new HeuristicHandler(this);
    }
}
