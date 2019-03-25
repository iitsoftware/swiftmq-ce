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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.mgmt.Configuration;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.CLIExecutor;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;

public class MgmtSwiftletImpl extends MgmtSwiftlet {
    SwiftletContext ctx = null;
    Listener listener = null;
    MessageInterfaceController miController = null;
    int toolCount = 0;

    public synchronized void fireEvent(boolean activated) {
        if (activated) {
            toolCount++;
            if (toolCount == 1)
                fireMgmtEvent(activated);
        } else {
            toolCount--;
            if (toolCount == 0)
                fireMgmtEvent(activated);
        }
    }

    public CLIExecutor createCLIExecutor() {
        return new CLIExecutorImpl(ctx);
    }

    protected void startup(Configuration config) throws SwiftletException {
        ctx = new SwiftletContext(this, config);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$routing", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    ctx.routingSwiftlet = (RoutingSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$routing");
                    ctx.routingSwiftlet.addRoutingListener(ctx.dispatchQueue);
                    miController = new MessageInterfaceController(ctx);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
            }
        });
        SwiftletManager.getInstance().addSwiftletManagerListener("sys$amqp", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent evt) {
                try {
                    listener = new Listener(ctx);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                }
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done");
    }

    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        if (ctx.routingSwiftlet != null) {
            miController.close();
            ctx.routingSwiftlet.removeRoutingListener(ctx.dispatchQueue);
        }
        ctx.close();
        listener.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done");
        ctx = null;
    }
}
