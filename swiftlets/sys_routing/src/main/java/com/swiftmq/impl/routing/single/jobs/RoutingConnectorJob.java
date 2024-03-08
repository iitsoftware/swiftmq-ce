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

package com.swiftmq.impl.routing.single.jobs;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import java.util.Properties;

public class RoutingConnectorJob implements Job {
    SwiftletContext ctx = null;
    Properties properties = null;
    String name = null;

    public RoutingConnectorJob(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private void doAction(boolean b) throws JobException {
        Entity entity = ctx.root.getEntity("connectors").getEntity(name);
        if (entity == null)
            throw new JobException("Routing Connector '" + name + "' is undefined!", null, false);
        Property prop = entity.getProperty("enabled");
        boolean enabled = (Boolean) prop.getValue();
        try {
            if (enabled != b)
                prop.setValue(b);
        } catch (Exception e) {
            throw new JobException(e.getMessage(), e, false);
        }
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        this.properties = properties;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/start ...");
        name = properties.getProperty("Connector Name");
        doAction(true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/start done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/stop ...");
        doAction(false);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[RoutingConnectorJob, properties=" + properties + "]";
    }
}
