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

package com.swiftmq.impl.routing.single.connection.stage;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.smqpr.CloseStageQueueRequest;
import com.swiftmq.impl.routing.single.smqpr.SMQRFactory;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.requestreply.Request;

import java.util.concurrent.atomic.AtomicBoolean;

public class StageQueue {
    SwiftletContext ctx = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    Stage stage = null;
    EventLoop eventLoop;

    public StageQueue(SwiftletContext ctx) {
        this.ctx = ctx;
        eventLoop = ctx.threadpoolSwiftlet.createEventLoop("sys$routing.connection.service", list -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/process, n=" + list.size());
            list.forEach(e -> {
                Request r = (Request) e;
                if (r.getDumpId() == SMQRFactory.CLOSE_STAGE_QUEUE_REQ) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/receiving: " + r);
                    close();
                    Semaphore sem = ((CloseStageQueueRequest) r).getSemaphore();
                    if (sem != null)
                        sem.notifySingleWaiter();
                    return;
                }
                if (stage != null)
                    stage.process((Request) e);
            });
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/process, done");
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue created");
    }

    public void enqueue(Object event) {
        eventLoop.submit(event);
    }

    public void closePreviousStage() {
        if (stage != null)
            stage.close();
    }

    public void setStage(Stage stage) {
        if (this.stage != null)
            closePreviousStage();
        this.stage = stage;
        if (stage != null) {
            stage.setStageQueue(this);
            stage.init();
        }
    }

    public Stage getStage() {
        return stage;
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/close...");
        eventLoop.close();
        if (stage != null)
            stage.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), "StageQueue/close done");
    }
}
