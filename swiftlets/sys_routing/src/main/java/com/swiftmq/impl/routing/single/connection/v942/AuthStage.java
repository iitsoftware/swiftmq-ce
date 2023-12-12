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

package com.swiftmq.impl.routing.single.connection.v942;

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.connection.stage.Stage;
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.impl.routing.single.smqpr.StartStageRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.AuthReplyRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.AuthRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.ConnectReplyRequest;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.requestreply.Request;

public class AuthStage extends Stage {
    ConnectReplyRequest connectReply = null;
    SMQRVisitor visitor = null;
    boolean listener = false;

    public AuthStage(SwiftletContext ctx, RoutingConnection routingConnection, ConnectReplyRequest connectReply) {
        super(ctx, routingConnection);
        this.connectReply = connectReply;
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, request -> {
            try {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + " ...");
                String password = routingConnection.getPassword();
                if (password == null)
                    throw new Exception("Authentication request by remote router but no password is defined!");
                ChallengeResponseFactory crf = (ChallengeResponseFactory) Class.forName(connectReply.getCrFactory()).newInstance();
                AuthRequest ar = new AuthRequest(crf.createResponse(connectReply.getChallenge(), password));
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", sending request: " + ar);
                routingConnection.getOutboundQueue().enqueue(ar);
                startValidTimer();
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", exception=" + e + ", disconnect");
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), AuthStage.this + "/exception: " + e.getMessage());
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.AUTH_REPREQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + "...");
            AuthReplyRequest reply = (AuthReplyRequest) request;
            if (reply.isOk()) {
                if (connectReply.isRequestXA() || routingConnection.isXa()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", launching recovery stage");
                    // Launch recovery stage
                    getStageQueue().setStage(new XARecoveryStage(ctx, routingConnection));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", launching non-xa delivery stage");
                    // Launch non-XA delivery stage
                    getStageQueue().setStage(new NonXADeliveryStage(ctx, routingConnection));
                }
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", disconnect");
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), AuthStage.this + "/exception: " + reply.getException());
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.AUTH_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request);
            AuthRequest pr = (AuthRequest) request;
            AuthReplyRequest reply = new AuthReplyRequest();
            if (ctx.challengeResponseFactory.verifyResponse(connectReply.getChallenge(), pr.getResponse(), routingConnection.getPassword())) {
                reply.setOk(true);
                if (connectReply.isRequestXA() || routingConnection.isXa()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", launching recovery stage");
                    // Launch recovery stage
                    getStageQueue().setStage(new XARecoveryStage(ctx, routingConnection));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", launching non-xa delivery stage");
                    // Launch non-XA delivery stage
                    getStageQueue().setStage(new NonXADeliveryStage(ctx, routingConnection));
                }
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/visited, request=" + request + ", invalid password, diconnect");
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), AuthStage.this + "/connection rejected, invalid password!");
                reply.setOk(false);
                reply.setException(new Exception("Invalid password!"));
                ctx.timerSwiftlet.addInstantTimerListener((Long) ctx.root.getProperty("reject-disconnect-delay").getValue(), new TimerListener() {
                    public void performTimeAction() {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), AuthStage.this + "/disconnect timeout");
                        ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                    }
                });
            }
            routingConnection.getOutboundQueue().enqueue(reply);
        });
        if (!listener)
            getStageQueue().enqueue(new StartStageRequest());
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
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.AUTH_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.AUTH_REPREQ, null);
    }

    public String toString() {
        return routingConnection.toString() + "/v942AuthStage";
    }
}
