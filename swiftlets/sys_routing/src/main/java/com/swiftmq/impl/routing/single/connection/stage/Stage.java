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
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.requestreply.Request;

public abstract class Stage {
    protected SwiftletContext ctx = null;
    protected RoutingConnection routingConnection = null;
    StageQueue stageQueue = null;
    TimerListener validTimer = null;
    boolean stageValid = true;

    public Stage(SwiftletContext ctx, RoutingConnection routingConnection) {
        this.ctx = ctx;
        this.routingConnection = routingConnection;
    }

    protected abstract void init();

    public StageQueue getStageQueue() {
        return stageQueue;
    }

    public void setStageQueue(StageQueue stageQueue) {
        this.stageQueue = stageQueue;
    }

    protected void startValidTimer() {
        validTimer = new TimerListener() {
            public void performTimeAction() {
                if (!stageValid)
                    return;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), Stage.this.toString() + "/stage valid timeout");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        };
        ctx.timerSwiftlet.addInstantTimerListener(((Long) ctx.root.getProperty("stage-valid-timeout").getValue()).longValue(), validTimer);
    }

    public abstract void process(Request request);

    public void close() {
        stageValid = false;
        if (validTimer != null)
            ctx.timerSwiftlet.removeTimerListener(validTimer);
    }
}
