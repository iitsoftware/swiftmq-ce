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
import com.swiftmq.impl.routing.single.smqpr.RequestHandler;
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
import java.util.Collections;
import java.util.Comparator;
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
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/created");
    }

    private List getPreparedXids(XidFilter filter) {
        List list = ctx.xaResourceManagerSwiftlet.getPreparedXids(filter);
        if (list == null || list.size() == 0)
            return null;
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                int i1 = ((XidImpl) o1).getFormatId();
                int i2 = ((XidImpl) o2).getFormatId();
                return i1 == i2 ? 0 : i1 < i2 ? -1 : 1;
            }
        });
        return list;
    }

    private boolean containsXid(List list, XidImpl xid) {
        if (list == null)
            return false;
        for (int i = 0; i < list.size(); i++) {
            if (xid.equals(((XidImpl) list.get(i))))
                return true;
        }
        return false;
    }

    private void doRecover(List localXids, List remoteXids) {
        // Nothing to recover
        if (localXids == null && remoteXids == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover, nothing to do");
            return;
        }

        List remoteRecoveryList = new ArrayList();
        // Check local vs remote
        if (localXids != null) {
            for (int i = 0; i < localXids.size(); i++) {
                XidImpl lXid = (XidImpl) localXids.get(i);
                if (containsXid(remoteXids, lXid)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover [" + lXid + "], local prepared, remote prepared, commit 1:local, 2:remote");
                    // local prepared, remote prepared, commit 1:local, 2:remote
                    XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(lXid);
                    try {
                        xac.commit(false);
                    } catch (XAContextException e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover, exception=" + e);
                        ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), toString() + "/doRecover, commit, exception=" + e);
                    }
                    ctx.xaResourceManagerSwiftlet.removeXAContext(lXid);
                    remoteRecoveryList.add(new CommitRequest(lXid));
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover [" + lXid + "], local prepared, remote unknown, rollback local");
                    // local prepared, remote unknown, rollback local
                    XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(lXid);
                    try {
                        xac.rollback();
                    } catch (XAContextException e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover, exception=" + e);
                        ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), toString() + "/doRecover, rollback, exception=" + e);
                    }
                    ctx.xaResourceManagerSwiftlet.removeXAContext(lXid);
                }
            }
        }

        // Check remote Xids not known locally
        if (remoteXids != null) {
            for (int i = 0; i < remoteXids.size(); i++) {
                XidImpl rXid = (XidImpl) remoteXids.get(i);
                if (!containsXid(localXids, rXid)) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/doRecover [" + rXid + "], remote prepared, locally already committed, commit remote");
                    // remote prepared, locally already committed, commit remote
                    remoteRecoveryList.add(new CommitRequest(rXid));
                }
            }
        }
        if (remoteRecoveryList.size() > 0) {
            Collections.sort(remoteRecoveryList, new Comparator() {
                public int compare(Object o1, Object o2) {
                    int i1 = ((CommitRequest) o1).getXid().getFormatId();
                    int i2 = ((CommitRequest) o2).getXid().getFormatId();
                    return i1 == i2 ? 0 : i1 < i2 ? -1 : 1;
                }
            });
            for (int i = 0; i < remoteRecoveryList.size(); i++) {
                CommitRequest r = (CommitRequest) remoteRecoveryList.get(i);
                routingConnection.getOutboundQueue().enqueue(r);
            }
        }
    }

    protected void init() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/init...");
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.SMQRFactory.START_STAGE_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + "...");
                RecoveryRequest rc = new RecoveryRequest();
                rc.setBranchQualifier(recoveryBranchQ);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", sending request=" + rc);
                routingConnection.getOutboundQueue().enqueue(rc);
                routingConnection.setXaSelected(true);
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REPREQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + "...");
                RecoveryReplyRequest reply = (RecoveryReplyRequest) request;
                if (reply.isOk()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", do recovery");
                    // do recovery
                    List localXids = getPreparedXids(new Filter(recoveryBranchQ));
                    List remoteXids = reply.getXidList();
                    doRecover(localXids, remoteXids);
                    localRecovered = true;

                    if (remoteRecovered) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", launching delivery stage");
                        getStageQueue().setStage(new XADeliveryStage(ctx, routingConnection));
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", start remote delivery");
                    // start delivery
                    routingConnection.getOutboundQueue().enqueue(new StartDeliveryRequest());
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", disconnect");
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/exception: " + reply.getException());
                    ctx.networkSwiftlet.getConnectionManager().removeConnection(routingConnection.getConnection());
                }
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.RECOVERY_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request);
                // A listener must wait until the connector sends a request.
                // It then sends a request by itself to ensure the XARecoveryStage is active at the connector side.
                if (listener)
                    getStageQueue().enqueue(new StartStageRequest());
                RecoveryRequest pr = (RecoveryRequest) request;
                RecoveryReplyRequest reply = new RecoveryReplyRequest();
                reply.setOk(true);
                // fill xid list
                reply.setXidList(getPreparedXids(new Filter(pr.getBranchQualifier())));
                routingConnection.getOutboundQueue().enqueue(reply);
                remoteRecovered = true;
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.COMMIT_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request);
                CommitRequest cr = (CommitRequest) request;
                XAContext xac = ctx.xaResourceManagerSwiftlet.getXAContext(cr.getXid());
                try {
                    xac.commit(false);
                } catch (XAContextException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visited, request=" + request + ", exception=" + e);
                    ctx.logSwiftlet.logError(ctx.routingSwiftlet.getName(), toString() + "/visited, request=" + request + ", exception=" + e);
                }
                ctx.xaResourceManagerSwiftlet.removeXAContext(cr.getXid());
            }
        });
        visitor.setRequestHandler(com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory.STARTDELIVERY_REQ, new RequestHandler() {
            public void visited(Request request) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), XARecoveryStage.this.toString() + "/visited, request=" + request + ", launching delivery stage");
                if (localRecovered && remoteRecovered)
                    getStageQueue().setStage(new XADeliveryStage(ctx, routingConnection));
            }
        });
        if (!listener)
            getStageQueue().enqueue(new StartStageRequest());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/init done");
    }

    public void process(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/process, request=" + request);
        request.accept(visitor);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close");
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

    private class Filter implements XidFilter {
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

