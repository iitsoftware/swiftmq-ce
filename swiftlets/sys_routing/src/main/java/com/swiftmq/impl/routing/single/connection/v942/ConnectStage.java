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

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.connection.stage.Stage;
import com.swiftmq.impl.routing.single.manager.po.POAddObject;
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.impl.routing.single.smqpr.StartStageRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.ConnectReplyRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.ConnectRequest;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.pipeline.POCallback;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.requestreply.Request;

public class ConnectStage extends Stage {
    SMQRVisitor visitor = null;
    boolean listener = false;

    public ConnectStage(SwiftletContext ctx, RoutingConnection routingConnection) {
        super(ctx, routingConnection);
        routingConnection.getSMQRFactory().setProtocolFactory(new com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory());
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/visited, request=" + request + "...");
            ConnectRequest cr = new ConnectRequest(ctx.routerName, routingConnection.isXa());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/visited, request=" + request + ", sending request= " + cr);
            routingConnection.getOutboundQueue().enqueue(cr);
            startValidTimer();
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.CONNECT_REPREQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/visited, request=" + request + "...");
            ConnectReplyRequest reply = (ConnectReplyRequest) request;
            if (reply.isOk()) {
                routingConnection.setRouterName(reply.getRouterName());
                ctx.connectionManager.enqueue(new POAddObject(new ConnectorCallback(reply), null, routingConnection));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/visited, request=" + request + ", disconnect");
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.CONNECT_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/visited, request=" + request);
            ConnectRequest pr = (ConnectRequest) request;
            routingConnection.setRouterName(pr.getRouterName());
            ctx.connectionManager.enqueue(new POAddObject(new ListenerCallback(pr), null, routingConnection));
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
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.CONNECT_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.CONNECT_REPREQ, null);
    }

    public String toString() {
        return routingConnection.toString() + "/v942ConnectStage";
    }

    private class ConnectorCallback implements POCallback {
        ConnectReplyRequest myReply = null;

        public ConnectorCallback(ConnectReplyRequest myReply) {
            this.myReply = myReply;
        }

        public void onSuccess(POObject po) {
            routingConnection.setKeepaliveInterval(myReply.getKeepAliveInterval());
            if (myReply.isAuthRequired()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ConnectorCallback.onSuccess, myreply=" + myReply + ", launching auth stage");
                // Launch auth stage
                getStageQueue().setStage(new AuthStage(ctx, routingConnection, myReply));
            } else {
                if (myReply.isRequestXA() || routingConnection.isXa()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ConnectorCallback.onSuccess, myreply=" + myReply + ", launching recovery stage");
                    // Launch recovery stage
                    getStageQueue().setStage(new XARecoveryStage(ctx, routingConnection));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ConnectorCallback.onSuccess, myreply=" + myReply + ", launching non-xa delivery stage");
                    // Launch non-XA delivery stage
                    getStageQueue().setStage(new NonXADeliveryStage(ctx, routingConnection));
                }

            }
        }

        public void onException(POObject po) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ConnectorCallback.onException, myreply=" + myReply + ", remove connection");
            ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), po.getException());
            System.err.println("+++ Routing Swiftlet: " + po.getException());
            ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
        }
    }

    private class ListenerCallback implements POCallback {
        ConnectRequest myRequest = null;

        public ListenerCallback(ConnectRequest myRequest) {
            this.myRequest = myRequest;
        }

        public void onSuccess(POObject po) {
            ConnectReplyRequest reply = new ConnectReplyRequest();
            reply.setOk(true);
            reply.setRouterName(ctx.routerName);
            reply.setKeepAliveInterval(routingConnection.getKeepaliveInterval());
            reply.setRequestXA(routingConnection.isXa());
            String password = routingConnection.getPassword();
            if (password != null) {
                reply.setAuthRequired(true);
                reply.setChallenge(ctx.challengeResponseFactory.createChallenge(password));
                reply.setCrFactory(ctx.challengeResponseFactory.getClass().getName());
            } else
                reply.setAuthRequired(false);
            if (reply.isAuthRequired()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ListenerCallback.onSuccess, myRequest=" + myRequest + ", launching auth stage");
                // Launch auth stage
                getStageQueue().setStage(new AuthStage(ctx, routingConnection, reply));
            } else {
                if (myRequest.isRequestXA() || routingConnection.isXa()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ListenerCallback.onSuccess, myRequest=" + myRequest + ", launching recovery stage");
                    // Launch recovery stage
                    getStageQueue().setStage(new XARecoveryStage(ctx, routingConnection));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ListenerCallback.onSuccess, myRequest=" + myRequest + ", launching non-xa delivery stage");
                    // Launch non-XA delivery stage
                    getStageQueue().setStage(new NonXADeliveryStage(ctx, routingConnection));
                }
            }
            routingConnection.getOutboundQueue().enqueue(reply);
        }

        public void onException(POObject po) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/ListenerCallback.onException, myRequest=" + myRequest + ", remove connection");
            ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), po.getException());
            System.err.println("+++ Routing Swiftlet: " + po.getException());
            ConnectReplyRequest reply = new ConnectReplyRequest();
            reply.setRouterName(ctx.routerName);
            reply.setOk(false);
            reply.setException(new Exception(po.getException()));
            routingConnection.getOutboundQueue().enqueue(reply);
            ctx.timerSwiftlet.addInstantTimerListener(((Long) ctx.root.getProperty("reject-disconnect-delay").getValue()).longValue(), new TimerListener() {
                public void performTimeAction() {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), ConnectStage.this + "/disconnect timeout");
                    ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                }
            });
        }
    }
}
