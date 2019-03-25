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

package com.swiftmq.jms.v510;

import com.swiftmq.jms.XACompletionListener;
import com.swiftmq.jms.XAResourceExtended;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.jms.smqp.v510.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XAResourceImpl implements XAResourceExtended {
    XASessionImpl session = null;
    int dispatchId;
    Map xidMapping = new HashMap();
    XACompletionListener completionListener = null;
    int nRecoverCalls = 0;
    PrintWriter logWriter = null;

    XAResourceImpl(XASessionImpl session) {
        this.session = session;
        session.session.setAutoAssign(false);
        dispatchId = session.getDispatchId();
    }

    public void setNeverSameRM(boolean neverSameRM) {
        // dummy
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
        if (logWriter != null) log(toString() + "/toSwiftMQXid, xid=" + xid + ", rXid=" + rXid);
        return rXid;
    }

    private Xid[] toArray(List xidList) {
        if (logWriter != null) log(toString() + "/toArray, xidList=" + xidList);
        if (xidList == null || xidList.size() == 0)
            return new Xid[0];
        Xid[] xids = new Xid[xidList.size()];
        for (int i = 0; i < xidList.size(); i++) {
            xids[i] = (Xid) xidList.get(i);
        }
        return xids;
    }

    private boolean isFlagSet(int flags, int flag) {
        return (flags & flag) == flag;
    }

    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    public PrintWriter getLogWriter() {
        return logWriter;
    }

    protected void log(String msg) {
        try {
            logWriter.println(msg);
            logWriter.flush();
        } catch (Exception e) {
        }
    }

    public synchronized void setCompletionListener(XACompletionListener completionListener) {
        this.completionListener = completionListener;
    }

    public String getRouterName() {
        return session.getSessionImpl().myConnection.metaData.getRouterName();
    }

    public synchronized boolean setTransactionTimeout(int seconds) throws XAException {
        if (logWriter != null) log(toString() + "/setTransactionTimeout, seconds=" + seconds);
        XAResSetTxTimeoutReply reply = null;

        try {
            reply = (XAResSetTxTimeoutReply) session.request(new XAResSetTxTimeoutRequest(dispatchId, (long) (seconds * 1000)));
            if (!reply.isOk())
                throw reply.getException();
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        return true;
    }

    public synchronized int getTransactionTimeout() throws XAException {
        XAResGetTxTimeoutReply reply = null;

        try {
            reply = (XAResGetTxTimeoutReply) session.request(new XAResGetTxTimeoutRequest(dispatchId));
            if (!reply.isOk())
                throw reply.getException();
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        int secs = (int) (reply.getTxTimeout() / 1000);
        if (logWriter != null) log(toString() + "/getTransactionTimeout, secs=" + secs);
        return secs;
    }


    public boolean isSameRM(XAResource resource) throws XAException {
        boolean b = false;
        if (resource instanceof XAResourceExtended) {
            XAResourceExtended that = (XAResourceExtended) resource;
            b = that.getRouterName().equals(getRouterName());
        }
        if (logWriter != null) log(toString() + "/isSameRM, resource=" + resource + ", returns=" + b);
        return b;
    }

    public synchronized Xid[] recover(int flag) throws XAException {
        if (isFlagSet(flag, XAResource.TMSTARTRSCAN)) {
            if (logWriter != null) log(toString() + "/TMSTARTRSCAN, flag=" + flag + ", nRecoverCalls=" + nRecoverCalls);
            nRecoverCalls = 0;
        } else if (logWriter != null)
            log(toString() + "/TMONOFLAGS, flag=" + flag + ", nRecoverCalls=" + nRecoverCalls);
        if (nRecoverCalls > 0)
            return new Xid[0];
        nRecoverCalls++;
        XAResRecoverReply reply = null;

        try {
            reply = (XAResRecoverReply) session.request(new XAResRecoverRequest(dispatchId, flag));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        if (reply.isOk())
            return toArray(reply.getXids());

        XAException ex = new XAException(reply.getException().getMessage());
        ex.errorCode = reply.getErrorCode();
        throw ex;
    }

    public synchronized void start(Xid xid, int flags) throws XAException {
        if (logWriter != null) log(toString() + "/start, xid=" + xid + ", flags=" + flags);
        XidImpl sxid = toSwiftMQXid(xid);
        XAResStartReply reply = null;

        try {
            reply = (XAResStartReply) session.request(new XAResStartRequest(dispatchId, sxid, flags));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        } else {
            try {
                session.session.assignLastMessage();
            } catch (Exception e) {
                XAException ex = new XAException(reply.getException().getMessage());
                ex.errorCode = reply.getErrorCode();
                throw ex;
            }
        }
        if (completionListener != null)
            completionListener.transactionStarted(sxid, session);
    }

    public synchronized void end(Xid xid, int flags) throws XAException {
        if (logWriter != null) log(toString() + "/end, xid=" + xid + ", flags=" + flags);
        XidImpl sxid = toSwiftMQXid(xid);
        XAResEndReply reply = null;

        try {
            List content = session.getAndClearCurrentTransaction();
            if (content != null && content.size() == 0)
                content = null;
            reply = (XAResEndReply) session.request(new XAResEndRequest(dispatchId, sxid, flags, content));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        if (!reply.isOk()) {
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        }
        if (completionListener != null)
            completionListener.transactionEnded(sxid);
    }

    public void forget(Xid xid) throws XAException {
        if (logWriter != null) log(toString() + "/forget, xid=" + xid);
    }

    public synchronized int prepare(Xid xid) throws XAException {
        if (logWriter != null) log(toString() + "/prepare, xid=" + xid);
        XidImpl sxid = toSwiftMQXid(xid);
        XAResPrepareReply reply = null;

        try {
            reply = (XAResPrepareReply) session.request(new XAResPrepareRequest(dispatchId, sxid));
        } catch (Exception e) {
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
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
        if (logWriter != null) log(toString() + "/commit, xid=" + xid + ", onePhase=" + onePhase);
        XidImpl sxid = toSwiftMQXid(xid);
        xidMapping.remove(xid);
        XAResCommitReply reply = null;

        try {
            XAResCommitRequest req = new XAResCommitRequest(dispatchId, sxid, onePhase);
            reply = (XAResCommitReply) session.request(req);
        } catch (Exception e) {
            if (completionListener != null)
                completionListener.transactionCommitted(sxid);
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        if (!reply.isOk()) {
            if (completionListener != null)
                completionListener.transactionCommitted(sxid);
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
            if (completionListener != null)
                completionListener.transactionCommitted(sxid);
        }
    }

    public synchronized void rollback(Xid xid) throws XAException {
        if (logWriter != null) log(toString() + "/rollback, xid=" + xid);
        XidImpl sxid = toSwiftMQXid(xid);
        xidMapping.remove(xid);
        XAResRollbackReply reply = null;

        try {
            session.getSessionImpl().startRecoverConsumers();
            session.getAndClearCurrentTransaction();
            reply = (XAResRollbackReply) session.request(new XAResRollbackRequest(dispatchId, sxid));
        } catch (Exception e) {
            if (completionListener != null)
                completionListener.transactionAborted(sxid);
            XAException ex = new XAException(e.toString());
            ex.errorCode = XAException.XAER_RMFAIL;
            throw ex;
        }
        if (reply.isOk()) {
            session.getSessionImpl().endRecoverConsumers();
            if (completionListener != null)
                completionListener.transactionAborted(sxid);
        } else {
            if (completionListener != null)
                completionListener.transactionAborted(sxid);
            XAException ex = new XAException(reply.getException().getMessage());
            ex.errorCode = reply.getErrorCode();
            throw ex;
        }
    }

}
