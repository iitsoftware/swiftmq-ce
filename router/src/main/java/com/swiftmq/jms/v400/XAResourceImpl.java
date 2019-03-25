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

package com.swiftmq.jms.v400;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.jms.smqp.v400.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XAResourceImpl implements XAResource {
    XASessionImpl session = null;
    int dispatchId;
    Map xidMapping = new HashMap();
    Map xidContent = new HashMap();

    XAResourceImpl(XASessionImpl session) {
        this.session = session;
        session.session.setAutoAssign(false);
        dispatchId = session.getDispatchId();
    }

    private XidImpl toSwiftMQXid(Xid xid) {
        XidImpl rXid = null;
        if (xid instanceof com.swiftmq.jms.XidImpl)
            rXid = (XidImpl) xid;
        else {
            rXid = (XidImpl) xidMapping.get(xid);
            if (rXid == null) {
                rXid = new XidImpl(xid);
                xidMapping.put(xid, rXid);
            }
        }
        return rXid;
    }

    private Xid[] toArray(List xidList) {
        if (xidList == null || xidList.size() == 0)
            return new Xid[0];
        Xid[] xids = new Xid[xidList.size()];
        for (int i = 0; i < xidList.size(); i++) {
            xids[i] = (Xid) xidList.get(i);
        }
        return xids;
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    public boolean isSameRM(XAResource resource) throws XAException {
        return this == resource;
    }

    public synchronized Xid[] recover(int flag) throws XAException {
        XAResRecoverReply reply = null;

        try {
            reply = (XAResRecoverReply) session.request(new XAResRecoverRequest(dispatchId, flag));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (reply.isOk())
            return toArray(reply.getXids());

        XAException ex = new XAException(reply.getException().getMessage());
        ex.errorCode = reply.getErrorCode();
        throw ex;
    }

    public synchronized void start(Xid xid, int flags) throws XAException {
        XidImpl sxid = toSwiftMQXid(xid);
        XAResStartReply reply = null;

        try {
            reply = (XAResStartReply) session.request(new XAResStartRequest(dispatchId, sxid, flags));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        } else {
            session.setCurrentTransaction((Object[]) xidContent.remove(sxid));
            try {
                session.session.assignLastMessage();
            } catch (Exception e) {
                XAException ex = new XAException(reply.getException().getMessage());
                ex.errorCode = reply.getErrorCode();
                throw ex;
            }
        }
    }

    public synchronized void end(Xid xid, int flags) throws XAException {
        XidImpl sxid = toSwiftMQXid(xid);
        XAResEndReply reply = null;

        try {
            reply = (XAResEndReply) session.request(new XAResEndRequest(dispatchId, sxid, flags));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        } else {
            Object[] content = session.getAndClearCurrentTransaction();
            if (content != null && content.length > 0)
                xidContent.put(sxid, content);
        }
    }

    public void forget(Xid xid) throws XAException {
        Xid sxid = toSwiftMQXid(xid);
        xidContent.remove(sxid);
    }

    public synchronized int prepare(Xid xid) throws XAException {
        XidImpl sxid = toSwiftMQXid(xid);
        XAResPrepareReply reply = null;

        try {
            reply = (XAResPrepareReply) session.request(new XAResPrepareRequest(dispatchId, sxid, (Object[]) xidContent.remove(sxid)));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        }
        return XA_OK;
    }

    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        XidImpl sxid = toSwiftMQXid(xid);
        xidMapping.remove(xid);
        XAResCommitReply reply = null;

        try {
            XAResCommitRequest req = new XAResCommitRequest(dispatchId, sxid, onePhase);
            if (onePhase)
                req.setMessages((Object[]) xidContent.remove(sxid));
            reply = (XAResCommitReply) session.request(req);
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        } else {
            if (reply.getDelay() > 0) {
                try {
                    Thread.sleep(reply.getDelay());
                } catch (Exception ignored) {
                }
            }
        }
    }

    public synchronized void rollback(Xid xid) throws XAException {
        XidImpl sxid = toSwiftMQXid(xid);
        xidMapping.remove(xid);
        XAResRollbackReply reply = null;

        try {
            session.getSessionImpl().startRecoverConsumers();
            xidContent.remove(sxid); // clear current transaction locally
            reply = (XAResRollbackReply) session.request(new XAResRollbackRequest(dispatchId, sxid));
        } catch (Exception e) {
            e.printStackTrace();
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XA_RBINTEGRITY;
            throw ex;
        }
        if (reply.isOk()) {
            session.getSessionImpl().endRecoverConsumers();
        } else {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        }
    }

}
