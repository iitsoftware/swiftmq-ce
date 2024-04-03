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
import com.swiftmq.impl.store.standard.processor.scan.po.StartScan;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;
import com.swiftmq.tools.concurrent.Semaphore;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScanJob implements Job {
    StoreContext ctx = null;
    final AtomicBoolean stopCalled = new AtomicBoolean(false);
    Properties properties = null;
    JobTerminationListener jobTerminationListener = null;

    public ScanJob(StoreContext ctx) {
        this.ctx = ctx;
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/start, properties=" + properties + " ...");
        this.jobTerminationListener = jobTerminationListener;
        this.properties = properties;
        Semaphore sem = new Semaphore();
        StartScan po = new StartScan(sem);
        ctx.scanProcessor.enqueue(po);
        sem.waitHere();
        if (po.isSuccess())
            jobTerminationListener.jobTerminated();
        else
            jobTerminationListener.jobTerminated(new JobException(po.getException(), new Exception(po.getException()), false));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/start, properties=" + properties + " ...");
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/stop ...");
        stopCalled.set(true);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[ScanJob, properties=" + properties + "]";
    }
}
