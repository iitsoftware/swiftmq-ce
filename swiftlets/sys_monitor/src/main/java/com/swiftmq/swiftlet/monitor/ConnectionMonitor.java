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

package com.swiftmq.swiftlet.monitor;

import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.Map;

public class ConnectionMonitor implements TimerListener {
    SwiftletContext ctx = null;
    EntityList networkUsage = null;
    Property connectionStartProp = null;
    boolean thresholdReached = false;

    public ConnectionMonitor(SwiftletContext ctx) {
        this.ctx = ctx;
        networkUsage = (EntityList) SwiftletManager.getInstance().getConfiguration("sys$net").getEntity("usage");
        connectionStartProp = ctx.root.getEntity("connection").getProperty("connection-threshold");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/created");
    }

    private boolean limitReached(int maxConnections) {
        if (maxConnections == -1)
            return false;
        Map entities = networkUsage.getEntities();
        return entities != null && entities.size() > maxConnections;
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction");
        if (limitReached(((Integer) connectionStartProp.getValue()).intValue())) {
            if (!thresholdReached) {
                ctx.mailGenerator.generateMail("Connection Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Number Connections CRITICAL!");
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, Number Connections CRITICAL!");
                ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Number Connections CRITICAL!");
            }
            thresholdReached = true;
        } else {
            if (thresholdReached) {
                ctx.mailGenerator.generateMail("Connection Monitor on " + SwiftletManager.getInstance().getRouterName() + ": Number Connections back to NORMAL!");
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/performTimeAction, Number Connections back to NORMAL!");
                ctx.logSwiftlet.logWarning(ctx.swiftlet.getName(), toString() + "/Number Connections back to NORMAL!");
            }
            thresholdReached = false;
        }
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.swiftlet.getName(), toString() + "/close");
    }

    public String toString() {
        return "ConnectionMonitor";
    }
}
