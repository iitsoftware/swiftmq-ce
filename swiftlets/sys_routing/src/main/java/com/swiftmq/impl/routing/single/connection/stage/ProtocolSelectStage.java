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
import com.swiftmq.impl.routing.single.smqpr.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.requestreply.Request;

public class ProtocolSelectStage extends Stage {
    boolean listener = false;
    SMQRVisitor visitor = null;

    public ProtocolSelectStage(SwiftletContext ctx, RoutingConnection routingConnection, boolean listener) {
        super(ctx, routingConnection);
        this.listener = listener;
        visitor = routingConnection.getVisitor();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(SMQRFactory.START_STAGE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + " ...");
                ProtocolRequest pr = new ProtocolRequest(StageFactory.getProtocolVersions());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + ", sending request...");
                routingConnection.getOutboundQueue().enqueue(pr);
                startValidTimer();
            }
        });
        visitor.setRequestHandler(SMQRFactory.PROTOCOL_REPREQ, request -> {
            ProtocolReplyRequest reply = (ProtocolReplyRequest) request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, reply=" + reply);
            if (reply.isOk()) {
                routingConnection.setProtocolVersion(reply.getProtocolVersionSelected());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, reply=" + reply + ", launching protocol stage");
                // Launch protocol stage
                getStageQueue().setStage(StageFactory.createFirstStage(ctx, routingConnection, reply.getProtocolVersionSelected()));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, reply=" + reply + ", disconnect");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(SMQRFactory.PROTOCOL_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request);
            ProtocolRequest pr = (ProtocolRequest) request;
            String selProt = StageFactory.selectProtocol(pr.getProtocolVersions());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + ", selected=" + selProt);
            ProtocolReplyRequest reply = new ProtocolReplyRequest();
            if (selProt != null) {
                routingConnection.setProtocolVersion(selProt);
                reply.setOk(true);
                reply.setProtocolVersionSelected(selProt);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + ", launching protocol stage");
                // Launch protocol stage
                getStageQueue().setStage(StageFactory.createFirstStage(ctx, routingConnection, selProt));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + ", nothing selected, timer-based disconnect (1s)");
                reply.setOk(false);
                reply.setException(new Exception("No matching protocol version found!"));
                ctx.timerSwiftlet.addInstantTimerListener(((Long) ctx.root.getProperty("reject-disconnect-delay").getValue()).longValue(), new TimerListener() {
                    public void performTimeAction() {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/disconnect timeout");
                        ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                    }
                });
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ProtocolSelectStage.this + "/visited, request=" + request + ", sending reply=" + reply);
            routingConnection.getOutboundQueue().enqueue(reply);
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init done");
    }

    public void process(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/process, request=" + request);
        request.accept(visitor);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/close");
        super.close();
        visitor.setRequestHandler(SMQRFactory.START_STAGE_REQ, null);
        visitor.setRequestHandler(SMQRFactory.PROTOCOL_REQ, null);
        visitor.setRequestHandler(SMQRFactory.PROTOCOL_REPREQ, null);
    }

    public String toString() {
        return routingConnection.toString() + "/ProtocolSelectStage";
    }
}
