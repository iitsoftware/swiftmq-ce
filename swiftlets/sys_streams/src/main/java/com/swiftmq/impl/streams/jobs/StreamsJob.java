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

package com.swiftmq.impl.streams.jobs;

import com.swiftmq.impl.streams.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import java.util.Properties;

public class StreamsJob implements Job {
    SwiftletContext ctx = null;
    Properties properties = null;
    String domainName = null;
    String packageName = null;
    String name = null;

    public StreamsJob(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private void doAction(boolean b) throws JobException {
        Entity domain = ctx.root.getEntity("domains").getEntity(domainName);
        if (domain == null)
            throw new JobException("Domain '" + domainName + "' is undefined!", null, false);
        Entity pkg = domain.getEntity("packages").getEntity(packageName);
        if (pkg == null)
            throw new JobException("Package '" + packageName + "' is undefined!", null, false);
        Entity entity = pkg.getEntity("streams").getEntity(name);
        if (entity == null)
            throw new JobException("Stream '" + name + "' is undefined!", null, false);
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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/start ...");
        domainName = properties.getProperty("Domain Name");
        packageName = properties.getProperty("Package Name");
        name = properties.getProperty("Stream Name");
        doAction(true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/start done");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/stop ...");
        doAction(false);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.streamsSwiftlet.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[StreamsJob, properties=" + properties + "]";
    }
}
