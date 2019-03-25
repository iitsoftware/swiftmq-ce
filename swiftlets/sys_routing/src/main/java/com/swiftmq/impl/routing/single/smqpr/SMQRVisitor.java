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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.tools.requestreply.GenericRequest;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

public class SMQRVisitor implements RequestVisitor {
    RequestHandler[] requestHandler = new RequestHandler[SMQRFactory.MAX_DUMP_ID];
    SwiftletContext ctx = null;
    RoutingConnection routingConnection = null;

    public SMQRVisitor(SwiftletContext ctx, RoutingConnection routingConnection) {
        this.ctx = ctx;
        this.routingConnection = routingConnection;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/created");
    }

    public void setRequestHandler(int dumpId, RequestHandler handler) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/setRequestHandler, dumpId=" + dumpId + ", handler=" + handler);
        requestHandler[dumpId] = handler;
    }

    public void visitGenericRequest(GenericRequest request) {
    }

    public void handleRequest(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/handleRequest, request=" + request);
        int dumpId = request.getDumpId();
        if (dumpId < 0 || dumpId > requestHandler.length - 1 || requestHandler[dumpId] == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/handleRequest, request=" + request + ", cannot handle it, no handler!");
            ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), toString() + "/handleRequest, request=" + request + ", cannot handle it, no handler, exiting!");
            ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/handleRequest, invoking handler=" + requestHandler[dumpId]);
            requestHandler[dumpId].visited(request);
        }
    }

    public String toString() {
        return routingConnection.toString() + "/SMQPRVisitor";
    }
}
