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

package com.swiftmq.impl.store.standard.processor.scan;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.log.CheckPointFinishedListener;
import com.swiftmq.impl.store.standard.processor.scan.po.Close;
import com.swiftmq.impl.store.standard.processor.scan.po.EventVisitor;
import com.swiftmq.impl.store.standard.processor.scan.po.StartScan;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;

public class ScanProcessor implements EventVisitor, CheckPointFinishedListener {
    static final String TP_SHRINK = "sys$store.processor";
    StoreContext ctx = null;
    boolean scanActive = false;
    EventLoop eventLoop;

    public ScanProcessor(StoreContext ctx) {
        this.ctx = ctx;
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$store.scan", list -> list.forEach(e -> ((POObject) e).accept(ScanProcessor.this)));
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/created");
    }

    // Called from the LogManager after a Checkpoint has been performed and before the Transaction Manager
    // is restarted
    public void checkpointFinished() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished ...");
        try {
            ctx.storeConverter.scanPageDB();
            SwiftletManager.getInstance().saveConfiguration();
            scanActive = false;
        } catch (Exception e) {
            e.printStackTrace();
            ctx.logSwiftlet.logError(ctx.storeSwiftlet.getName(), toString() + "/exception during scan: " + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/checkpointFinished done");
    }

    public void enqueue(POObject po) {
        eventLoop.submit(po);
    }

    public void visit(StartScan po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/" + po + " ...");
        if (scanActive) {
            // reject it
            String msg = "Can't start scan: another scan is active right now!";
            po.setException(msg);
            po.setSuccess(false);
        } else {
            scanActive = true;
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
        eventLoop.submit(new Close(sem));
        sem.waitHere();
        eventLoop.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.storeSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "ScanProcessor";
    }
}
