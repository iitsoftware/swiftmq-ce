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
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;

public class JobRegistrar {
    SwiftletContext ctx = null;
    EntityList entityList = null;
    JobGroup jobGroup = null;

    public JobRegistrar(SwiftletContext ctx, EntityList entityList) {
        this.ctx = ctx;
        this.entityList = entityList;
    }

    public void register() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/register ...");
        jobGroup = ctx.schedulerSwiftlet.getJobGroup("JMS Bridge");
        JobFactory jf = new ServerBridgeJobFactory(ctx, entityList);
        jobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/register done");
    }

    public void unregister() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/unregister ...");
        jobGroup.removeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("xt$bridge", toString() + "/unregister done");
    }
}
