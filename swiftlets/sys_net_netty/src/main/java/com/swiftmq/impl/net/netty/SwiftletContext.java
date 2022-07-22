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

package com.swiftmq.impl.net.netty;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.util.concurrent.atomic.AtomicLong;

public class SwiftletContext {
    public static final String TP_CONNMGR = "sys$net.connection.mgr";
    public static final String TP_CONNHANDLER = "sys$net.connection.handler";
    public Configuration config = null;
    public Entity root = null;
    public EntityList usageList = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public LogSwiftlet logSwiftlet = null;
    public MgmtSwiftlet mgmtSwiftlet = null;
    public NetworkSwiftletImpl networkSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public AtomicLong maxChunkSize = new AtomicLong();

    public SwiftletContext(Configuration config, NetworkSwiftletImpl networkSwiftlet) throws Exception {
        this.config = config;
        this.networkSwiftlet = networkSwiftlet;
        root = config;
        usageList = (EntityList) root.getEntity("usage");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
        threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
        maxChunkSize.set((Long) root.getProperty("max-chunk-size").getValue());
        root.getProperty("max-chunk-size").setPropertyChangeListener((property, oldValue, newValue) -> maxChunkSize.set((Long) newValue));
    }
}
