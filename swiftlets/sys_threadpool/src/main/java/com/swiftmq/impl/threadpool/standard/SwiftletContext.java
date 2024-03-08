/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.threadpool.standard;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class SwiftletContext {
    public Entity config = null;
    public Entity usage = null;

    public ThreadpoolSwiftletImpl threadpoolSwiftlet;
    public MgmtSwiftlet mgmtSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public LogSwiftlet logSwiftlet;
    public TraceSwiftlet traceSwiftlet;
    public TraceSpace traceSpace;

    public SwiftletContext(Entity config, ThreadpoolSwiftletImpl threadpoolSwiftlet) {
        this.config = config;
        this.threadpoolSwiftlet = threadpoolSwiftlet;
        usage = ((EntityList) config.getEntity("usage")).createEntity();
        usage.setName("Threads");
        usage.createCommands();
        try {
            config.getEntity("usage").addEntity(usage);
        } catch (EntityAddException e) {
            throw new RuntimeException(e);
        }

        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);

    }
}
