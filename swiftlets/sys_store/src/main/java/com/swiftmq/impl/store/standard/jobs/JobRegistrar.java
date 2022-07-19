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

package com.swiftmq.impl.store.standard.jobs;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;

public class JobRegistrar {
    StoreContext ctx = null;
    JobGroup jobGroup = null;

    public JobRegistrar(StoreContext ctx) {
        this.ctx = ctx;
    }

    public void register() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/register ...");
        jobGroup = ctx.schedulerSwiftlet.getJobGroup("Store");
        JobFactory jf = new BackupJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new ShrinkJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        jf = new RecommendJobFactory(ctx);
        jobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/register done");
    }

    public void unregister() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/unregister ...");
        jobGroup.removeAll();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/unregister done");
    }
}
