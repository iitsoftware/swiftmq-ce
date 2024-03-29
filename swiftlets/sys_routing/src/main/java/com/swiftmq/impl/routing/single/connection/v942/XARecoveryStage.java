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
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.impl.routing.single.smqpr.StartStageRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.CommitRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.RecoveryReplyRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.RecoveryRequest;
import com.swiftmq.impl.routing.single.smqpr.v942.StartDeliveryRequest;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.xa.XAContext;
import com.swiftmq.swiftlet.xa.XAContextException;
import com.swiftmq.swiftlet.xa.XidFilter;
import com.swiftmq.tools.prop.ParSub;
import com.swiftmq.tools.requestreply.Request;

import java.util.ArrayList;
import java.util.List;

public class XARecoveryStage extends Stage {
    public static final String XID_BRANCH = "swiftmq/src=$0/dest=$1";

    SMQRVisitor visitor = null;
    boolean listener = false;
    String recoveryBranchQ = null;
    boolean localRecovered = false;
    boolean remoteRecovered = false;

    public XARecoveryStage(SwiftletContext ctx, RoutingConnection routingConnection) {
        super(ctx, routingConnection);
        visitor = routingConnection.getVisitor();
        listener = routingConnection.isListener();
        recoveryBranchQ = ParSub.substitute(XID_BRANCH, new String[]{ctx.routerName, routingConnection.getRouterName()});
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/created");
    }

    private List getPreparedXids(XidFilter filter) {
        List list = ctx.xaResourceManagerSwiftlet.getPreparedXids(filter);
        if (list == null || list.isEmpty())
            return null;
        list.sort((o1, o2) -> {
            int i1 = ((XidImpl) o1).getFormatId();
            int i2 = ((XidImpl) o2).getFormatId();
            return Integer.compare(i1, i2);
        });
        return list;
    }

    private boolean containsXid(List list, XidImpl xid) {
        if (list == null)
            return false;
        for (Object o : list) {
            if (xid.equals(o))
                return true;
        }
        return false;
    }

    private void doRecover(List localXids, List remoteXids) {
        // Nothing to recover
        if (localXids == null && remoteXids == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover, nothing to do");
            return;
        }

        List<CommitRequest> remoteRecoveryList = new ArrayList();
        // Check local vs remote
        if (localXids != null) {
            for (Object localXid : localXids) {
                XidImpl lXid = (XidImpl) localXid;
                if (containsXid(remoteXids, lXid)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover [" + lXid + "], local prepared, remote prepared, commit 1:local, 2:remote");
                    // local prepared, remote prepared, commit 1:local, 2:remote
                    XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(lXid);
                    try {
                        xac.commit(false);
                    } catch (XAContextException e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover, exception=" + e);
                        ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/doRecover, commit, exception=" + e);
                    }
                    ctx.xaResourceManagerSwiftlet.removeXAContext(lXid);
                    remoteRecoveryList.add(new CommitRequest(lXid));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover [" + lXid + "], local prepared, remote unknown, rollback local");
                    // local prepared, remote unknown, rollback local
                    XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(lXid);
                    try {
                        xac.rollback();
                    } catch (XAContextException e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover, exception=" + e);
                        ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/doRecover, rollback, exception=" + e);
                    }
                    ctx.xaResourceManagerSwiftlet.removeXAContext(lXid);
                }
            }
        }

        // Check remote Xids not known locally
        if (remoteXids != null) {
            for (Object remoteXid : remoteXids) {
                XidImpl rXid = (XidImpl) remoteXid;
                if (!containsXid(localXids, rXid)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/doRecover [" + rXid + "], remote prepared, locally already committed, commit remote");
                    // remote prepared, locally already committed, commit remote
                    remoteRecoveryList.add(new CommitRequest(rXid));
                }
            }
        }
        if (!remoteRecoveryList.isEmpty()) {
            remoteRecoveryList.sort((o1, o2) -> {
                int i1 = o1.getXid().getFormatId();
                int i2 = o2.getXid().getFormatId();
                return Integer.compare(i1, i2);
            });
            remoteRecoveryList.forEach(commitRequest -> routingConnection.getOutboundQueue().submit(commitRequest));
        }
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + "...");
            RecoveryRequest rc = new RecoveryRequest();
            rc.setBranchQualifier(recoveryBranchQ);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", sending request=" + rc);
            routingConnection.getOutboundQueue().submit(rc);
            routingConnection.setXaSelected(true);
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REPREQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + "...");
            RecoveryReplyRequest reply = (RecoveryReplyRequest) request;
            if (reply.isOk()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", do recovery");
                // do recovery
                List localXids = getPreparedXids(new Filter(recoveryBranchQ));
                List remoteXids = reply.getXidList();
                doRecover(localXids, remoteXids);
                localRecovered = true;

                if (remoteRecovered) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", launching delivery stage");
                    getStageQueue().setStage(new XADeliveryStage(ctx, routingConnection));
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", start remote delivery");
                // start delivery
                routingConnection.getOutboundQueue().submit(new StartDeliveryRequest());
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", disconnect");
                ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/exception: " + reply.getException());
                ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request);
            // A listener must wait until the connector sends a request.
            // It then sends a request by itself to ensure the XARecoveryStage is active at the connector side.
            if (listener)
                getStageQueue().enqueue(new StartStageRequest());
            RecoveryRequest pr = (RecoveryRequest) request;
            RecoveryReplyRequest reply = new RecoveryReplyRequest();
            reply.setOk(true);
            // fill xid list
            reply.setXidList(getPreparedXids(new Filter(pr.getBranchQualifier())));
            routingConnection.getOutboundQueue().submit(reply);
            remoteRecovered = true;
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request);
                CommitRequest cr = (CommitRequest) request;
                XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(cr.getXid());
                try {
                    xac.commit(false);
                } catch (XAContextException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/visited, request=" + request + ", exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), this + "/visited, request=" + request + ", exception=" + e);
                }
                ctx.xaResourceManagerSwiftlet.removeXAContext(cr.getXid());
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.STARTDELIVERY_REQ, request -> {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this + "/visited, request=" + request + ", launching delivery stage");
            if (localRecovered && remoteRecovered)
                getStageQueue().setStage(new XADeliveryStage(ctx, routingConnection));
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
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REPREQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REQ, null);
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.STARTDELIVERY_REQ, null);
    }

    public String toString() {
        return routingConnection.toString() + "/v942XARecoveryStage, recoveryBranchQ=" + recoveryBranchQ;
    }

    private static class Filter implements XidFilter {
        String branchQ = null;

        public Filter(String branchQ) {
            this.branchQ = branchQ;
        }

        public boolean isMatch(XidImpl xid) {
            if (!xid.isRouting())
                return false;
            byte[] bq = xid.getBranchQualifier();
            if (bq == null)
                return false;
            String sbq = new String(bq);
            return sbq.equals(branchQ);
        }
    }
}

