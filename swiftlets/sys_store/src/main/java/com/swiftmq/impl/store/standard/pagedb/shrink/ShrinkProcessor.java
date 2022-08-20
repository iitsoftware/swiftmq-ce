/*
 * Copyright 2022 IIT Software GmbH
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

package com.swiftmq.impl.store.standard.pagedb.shrink;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.log.CheckPointFinishedListener;
import com.swiftmq.impl.store.standard.pagedb.shrink.po.Close;
import com.swiftmq.impl.store.standard.pagedb.shrink.po.EventVisitor;
import com.swiftmq.impl.store.standard.pagedb.shrink.po.StartShrink;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;

public class ShrinkProcessor implements EventVisitor, CheckPointFinishedListener {
    static final String TP_SHRINK = "sys$store.shrink";
    StoreContext ctx = null;
    PipelineQueue pipelineQueue = null;
    boolean shrinkActive = false;

    public ShrinkProcessor(StoreContext ctx) {
        this.ctx = ctx;
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_SHRINK), TP_SHRINK, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/created");
    }

    // Called from the LogManager after a Checkpoint has been performed and before the Transaction Manager
    // is restarted
    public void checkpointFinished() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished ...");
        try {
            ctx.cacheManager.shrink();
            ctx.stableStore.shrink();
            shrinkActive = false;
        } catch (Exception e) {
            e.printStackTrace();
            ctx.logSwiftlet.logError(ctx.storeSwiftlet.getName(), toString() + "/exception during shrink: " + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished done");
    }

    public void enqueue(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void visit(StartShrink po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (shrinkActive) {
            // reject it
            String msg = "Can't start Shrink: another Shrink is active right now!";
            po.setException(msg);
            po.setSuccess(false);
        } else {
            shrinkActive = true;
            ctx.transactionManager.initiateCheckPoint(this);
            po.setSuccess(true);
        }
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(Close po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/close ...");
        Semaphore sem = new Semaphore();
        pipelineQueue.enqueue(new Close(sem));
        sem.waitHere();
        pipelineQueue.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "ShrinkProcessor";
    }
}
