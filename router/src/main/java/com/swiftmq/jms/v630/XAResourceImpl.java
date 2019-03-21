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

package com.swiftmq.jms.v630;

import com.swiftmq.jms.XACompletionListener;
import com.swiftmq.jms.XAResourceExtended;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.jms.smqp.v630.*;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.ValidationException;

import javax.jms.JMSException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XAResourceImpl implements XAResourceExtended, RequestRetryValidator
{
  private static final SimpleDateFormat format = new SimpleDateFormat("yyMMdd:HH:mm:ss.SSS");
  XASessionImpl session = null;
  Map xidMapping = new HashMap();
  XACompletionListener completionListener = null;
  int nRecoverCalls = 0;
//  PrintWriter logWriter = new PrintWriter(new OutputStreamWriter(System.out));
  PrintWriter logWriter = null;
  int lastEndRequestConnectionId = -1;
  boolean neverSameRM = false;

  XAResourceImpl(XASessionImpl session)
  {
    this.session = session;
    session.getSessionImpl().setAutoAssign(false);
    session.getSessionImpl().setXaMode(true);
  }

  public void setNeverSameRM(boolean neverSameRM)
  {
    this.neverSameRM = neverSameRM;
    if (logWriter != null) log(toString() + "/setNeverSameRM, neverSameRM=" + neverSameRM);
  }

  private boolean endRequestInDoubt()
  {
    boolean rc = false;
    int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
    if (lastEndRequestConnectionId == -1 || connectionId == -1)
      rc = false;
    else
      rc = lastEndRequestConnectionId < connectionId;
    if (logWriter != null)
      log(toString() + "/endRequestInDoubt, rc=" + rc + ", lastEndRequestConnectionId" + lastEndRequestConnectionId + ", connectionId=" + connectionId);
    return rc;
  }

  public void validate(Request request) throws ValidationException
  {
    if (logWriter != null) log(toString() + "/validate, request=" + request + " ...");
    request.setDispatchId(session.getDispatchId());
    request.setConnectionId(session.getSessionImpl().getMyConnection().getConnectionId());
    if (request instanceof XAResStartRequest)
    {
      XAResStartRequest r = (XAResStartRequest) request;
      r.setRetry(true);
      List l = XARecoverRegistry.getInstance().getRequestList(r.getXid());
      r.setRecoverRequestList(l);
    } else if (request instanceof XAResEndRequest)
    {
      XAResEndRequest r = (XAResEndRequest) request;
      r.setRetry(true);
      List l = XARecoverRegistry.getInstance().getRequestList(r.getXid());
      r.setRecoverRequestList(l);
    } else if (request instanceof XAResPrepareRequest)
    {
      XAResPrepareRequest r = (XAResPrepareRequest) request;
      r.setRetry(true);
      List l = XARecoverRegistry.getInstance().getRequestList(r.getXid());
      r.setRecoverRequestList(l);
    } else if (request instanceof XAResCommitRequest)
    {
      XAResCommitRequest r = (XAResCommitRequest) request;
      r.setRetry(true);
      List l = XARecoverRegistry.getInstance().getRequestList(r.getXid());
      r.setRecoverRequestList(l);
    } else if (request instanceof XAResRollbackRequest)
    {
      XAResRollbackRequest r = (XAResRollbackRequest) request;
      r.setRetry(true);
      List l = XARecoverRegistry.getInstance().getRequestList(r.getXid());
      r.setRecoverRequestList(l);
    }
    else if (request instanceof XAResForgetRequest)
    {
      XAResForgetRequest r = (XAResForgetRequest) request;
      r.setRetry(true);
    }
    if (logWriter != null) log(toString() + "/validate, request=" + request + " done");
  }

  private XidImpl toSwiftMQXid(Xid xid)
  {
    XidImpl rXid = null;
    if (xid instanceof com.swiftmq.jms.XidImpl)
      rXid = (XidImpl) xid;
    else
    {
      rXid = (XidImpl) xidMapping.get(xid);
      if (rXid == null)
      {
        rXid = new XidImpl(xid);
        xidMapping.put(xid, rXid);
      }
    }
    if (logWriter != null) log(toString() + "/toSwiftMQXid, xid=" + xid + ", rXid=" + rXid);
    return rXid;
  }

  private Xid[] toArray(List xidList)
  {
    if (logWriter != null) log(toString() + "/toArray, xidList=" + xidList);
    if (xidList == null || xidList.size() == 0)
      return new Xid[0];
    Xid[] xids = new Xid[xidList.size()];
    for (int i = 0; i < xidList.size(); i++)
    {
      xids[i] = (Xid) xidList.get(i);
    }
    return xids;
  }

  private boolean isFlagSet(int flags, int flag)
  {
    return (flags & flag) == flag;
  }

  public void setLogWriter(PrintWriter logWriter)
  {
    this.logWriter = logWriter;
  }

  public PrintWriter getLogWriter()
  {
    return logWriter;
  }

  protected void log(String msg)
  {
    try
    {
      logWriter.println(format.format(new Date()) + "/" + msg);
      logWriter.flush();
    } catch (Exception e)
    {
    }
  }

  public synchronized void setCompletionListener(XACompletionListener completionListener)
  {
    this.completionListener = completionListener;
  }

  public String getRouterName()
  {
    return session.getSessionImpl().myConnection.metaData.getRouterName();
  }

  public synchronized boolean setTransactionTimeout(int seconds) throws XAException
  {
    if (logWriter != null) log(toString() + "/setTransactionTimeout, seconds=" + seconds);
    XAResSetTxTimeoutReply reply = null;

    try
    {
      reply = (XAResSetTxTimeoutReply) session.request(new XAResSetTxTimeoutRequest(this, session.getDispatchId(), (long) (seconds * 1000)));
      if (!reply.isOk())
        throw reply.getException();
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    return true;
  }

  public synchronized int getTransactionTimeout() throws XAException
  {
    XAResGetTxTimeoutReply reply = null;

    try
    {
      reply = (XAResGetTxTimeoutReply) session.request(new XAResGetTxTimeoutRequest(this, session.getDispatchId()));
      if (!reply.isOk())
        throw reply.getException();
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    int secs = (int) (reply.getTxTimeout() / 1000);
    if (logWriter != null) log(toString() + "/getTransactionTimeout, secs=" + secs);
    return secs;
  }


  public boolean isSameRM(XAResource resource) throws XAException
  {
    if (logWriter != null) log(toString() + "/isSameRM, neverSameRM=" + neverSameRM);
    if (neverSameRM)
      return false;
    boolean b = false;
    if (resource instanceof XAResourceExtended)
    {
      XAResourceExtended that = (XAResourceExtended) resource;
      b = that.getRouterName().equals(getRouterName());
    }
    if (logWriter != null) log(toString() + "/isSameRM, resource=" + resource + ", returns=" + b);
    return b;
  }

  public synchronized Xid[] recover(int flag) throws XAException
  {
    if (isFlagSet(flag, XAResource.TMSTARTRSCAN))
    {
      if (logWriter != null) log(toString() + "/TMSTARTRSCAN, flag=" + flag + ", nRecoverCalls=" + nRecoverCalls);
      nRecoverCalls = 0;
    } else if (logWriter != null) log(toString() + "/TMONOFLAGS, flag=" + flag + ", nRecoverCalls=" + nRecoverCalls);
    if (nRecoverCalls > 0)
      return new Xid[0];
    nRecoverCalls++;
    XAResRecoverReply reply = null;

    try
    {
      reply = (XAResRecoverReply) session.request(new XAResRecoverRequest(this, session.getDispatchId(), flag));
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (reply.isOk())
      return toArray(reply.getXids());

    XAException ex = new XAException(reply.getException().getMessage());
    if (reply.getErrorCode() != 0)
      ex.errorCode = reply.getErrorCode();
    else
      ex.errorCode = XAException.XAER_RMFAIL;
    throw ex;
  }

  public synchronized void start(Xid xid, int flags) throws XAException
  {
    if (logWriter != null) log(toString() + "/start, xid=" + xid + ", flags=" + flags);
    XidImpl sxid = toSwiftMQXid(xid);
    XAResStartReply reply = null;

    int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
    Request request = new XAResStartRequest(this, session.getDispatchId(), sxid, flags, false, null);
    request.setConnectionId(connectionId);
    try
    {
      reply = (XAResStartReply) session.request(request);
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (!reply.isOk())
    {
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    } else
    {
      XARecoverRegistry.getInstance().addRequest(sxid, request);
      try
      {
        session.session.assignLastMessage();
      } catch (Exception e)
      {
        XAException ex = new XAException(reply.getException().getMessage());
        if (reply.getErrorCode() != 0)
          ex.errorCode = reply.getErrorCode();
        else
          ex.errorCode = XAException.XAER_RMFAIL;
        throw ex;
      }
    }
    if (completionListener != null)
      completionListener.transactionStarted(sxid, session);
  }

  public synchronized void end(Xid xid, int flags) throws XAException
  {
    if (logWriter != null) log(toString() + "/end, xid=" + xid + ", flags=" + flags);
    XidImpl sxid = toSwiftMQXid(xid);
    XAResEndReply reply = null;
    XAResEndRequest request = null;

    try
    {
      int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
      List content = session.getAndClearCurrentTransaction();
      if (content != null && content.size() == 0)
        content = null;
      request = new XAResEndRequest(this, session.getDispatchId(), sxid, flags, false, content, XARecoverRegistry.getInstance().getRequestList(sxid));

      request.setConnectionId(connectionId);
      lastEndRequestConnectionId = connectionId;
      reply = (XAResEndReply) session.request(request);
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (!reply.isOk())
    {
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    request.setRecoverRequestList(null);
    XARecoverRegistry.getInstance().addRequest(sxid, request);
    if (completionListener != null)
      completionListener.transactionEnded(sxid);
  }

  public synchronized void forget(Xid xid) throws XAException
  {
    if (logWriter != null) log(toString() + "/forget, xid=" + xid);
    XidImpl sxid = toSwiftMQXid(xid);
    xidMapping.remove(xid);
    XAResForgetReply reply = null;

    try
    {
      int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
      Request request = new XAResForgetRequest(this, session.getDispatchId(), sxid, false);
      request.setConnectionId(connectionId);
      reply = (XAResForgetReply) session.request(request);
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (!reply.isOk())
    {
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
  }

  public synchronized int prepare(Xid xid) throws XAException
  {
    if (logWriter != null) log(toString() + "/prepare, xid=" + xid);
    XidImpl sxid = toSwiftMQXid(xid);
    XAResPrepareReply reply = null;

    try
    {
      int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
      Request request = new XAResPrepareRequest(this, session.getDispatchId(), sxid, false, endRequestInDoubt() ? XARecoverRegistry.getInstance().getRequestList(sxid) : null);
      request.setConnectionId(connectionId);
      reply = (XAResPrepareReply) session.request(request);
    } catch (Exception e)
    {
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (!reply.isOk())
    {
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    XARecoverRegistry.getInstance().clear(sxid);
    return XA_OK;
  }

  public synchronized void commit(Xid xid, boolean onePhase) throws XAException
  {
    if (logWriter != null) log(toString() + "/commit, xid=" + xid + ", onePhase=" + onePhase);
    XidImpl sxid = toSwiftMQXid(xid);
    xidMapping.remove(xid);
    XAResCommitReply reply = null;

    try
    {
      int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
      XAResCommitRequest req = new XAResCommitRequest(this, session.getDispatchId(), sxid, onePhase, false, onePhase && endRequestInDoubt() ? XARecoverRegistry.getInstance().getRequestList(sxid) : null);
      req.setConnectionId(connectionId);
      reply = (XAResCommitReply) session.request(req);
    } catch (Exception e)
    {
      if (completionListener != null)
        completionListener.transactionCommitted(sxid);
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (!reply.isOk())
    {
      if (completionListener != null)
        completionListener.transactionCommitted(sxid);
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    } else
    {
      try
      {
        session.getSessionImpl().afterCommit();
      } catch (JMSException e)
      {
        XAException ex = new XAException(e.toString());
        ex.errorCode = XAException.XAER_RMFAIL;
        throw ex;
      }
      if (reply.getDelay() > 0)
      {
        try
        {
          Thread.sleep(reply.getDelay());
        } catch (Exception ignored)
        {
        }
      }
      if (completionListener != null)
        completionListener.transactionCommitted(sxid);
      XARecoverRegistry.getInstance().clear(sxid);
    }
  }

  public synchronized void rollback(Xid xid) throws XAException
  {
    if (logWriter != null) log(toString() + "/rollback, xid=" + xid);
    XidImpl sxid = toSwiftMQXid(xid);
    xidMapping.remove(xid);
    XAResRollbackReply reply = null;

    try
    {
      int connectionId = session.getSessionImpl().getMyConnection().getConnectionId();
      session.getSessionImpl().startRecoverConsumers();
      session.getAndClearCurrentTransaction();
      List recoveryList = null;
      if (endRequestInDoubt())
        recoveryList = XARecoverRegistry.getInstance().getRequestList(sxid);
      Request request = new XAResRollbackRequest(this, session.getDispatchId(), sxid, false, recoveryList, session.getSessionImpl().getRecoveryEpoche());
      request.setConnectionId(connectionId);
      reply = (XAResRollbackReply) session.request(request);
    } catch (Exception e)
    {
      if (completionListener != null)
        completionListener.transactionAborted(sxid);
      XAException ex = new XAException(e.toString());
      ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
    if (reply.isOk())
    {
      session.getSessionImpl().endRecoverConsumersXA();
      try
      {
        session.getSessionImpl().closeDelayedProducers();
      } catch (JMSException e)
      {
        e.printStackTrace();
      }
      if (completionListener != null)
        completionListener.transactionAborted(sxid);
      XARecoverRegistry.getInstance().clear(sxid);
    } else
    {
      if (completionListener != null)
        completionListener.transactionAborted(sxid);
      XAException ex = new XAException(reply.getException().getMessage());
      if (reply.getErrorCode() != 0)
        ex.errorCode = reply.getErrorCode();
      else
        ex.errorCode = XAException.XAER_RMFAIL;
      throw ex;
    }
  }

}
