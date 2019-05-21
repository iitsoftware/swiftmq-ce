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

package com.swiftmq.impl.jmsbridge.jobs;

import com.swiftmq.impl.jmsbridge.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import java.util.Properties;

public class ServerBridgeJob implements Job {
    SwiftletContext ctx = null;
    EntityList entityList = null;
    Properties properties = null;
    String name = null;

    public ServerBridgeJob(SwiftletContext ctx, EntityList entityList) {
        this.ctx = ctx;
        this.entityList = entityList;
    }

    private void doAction(boolean b) throws JobException {
        Entity entity = entityList.getEntity(name);
        if (entity == null)
            throw new JobException("Server Bridge '" + name + "' is undefined!", null, false);
        Property prop = entity.getProperty("enabled");
        boolean enabled = ((Boolean) prop.getValue()).booleanValue();
        try {
            if (enabled != b)
                prop.setValue(new Boolean(b));
        } catch (Exception e) {
            throw new JobException(e.getMessage(), e, false);
        }
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        this.properties = properties;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/start ...");
        name = properties.getProperty("Bridge Name");
        doAction(true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/start done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/stop ...");
        doAction(false);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/stop done");
    }

    public String toString() {
        return "[ServerBridgeJob, properties=" + properties + "]";
    }
}
