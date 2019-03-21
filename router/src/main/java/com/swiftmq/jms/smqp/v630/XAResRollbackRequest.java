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

package com.swiftmq.jms.smqp.v630;

/** SMQP-Protocol Version 630, Class: XAResRollbackRequest
 *  Automatically generated, don't change!
 *  Generation Date: Thu Aug 30 17:17:54 CEST 2007
 *  (c) 2007, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class XAResRollbackRequest extends Request
{
  private XidImpl xid;
  private boolean retry;
  private List recoverRequestList;
  private int recoveryEpoche;

  public XAResRollbackRequest()
  {
    super(0, true);
  }

  public XAResRollbackRequest(int dispatchId)
  {
    super(dispatchId, true);
  }

  public XAResRollbackRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId, true, validator);
  }

  public XAResRollbackRequest(int dispatchId, XidImpl xid, boolean retry, List recoverRequestList, int recoveryEpoche)
  {
    super(dispatchId, true);
    this.xid = xid;
    this.retry = retry;
    this.recoverRequestList = recoverRequestList;
    this.recoveryEpoche = recoveryEpoche;
  }

  public XAResRollbackRequest(RequestRetryValidator validator, int dispatchId, XidImpl xid, boolean retry, List recoverRequestList, int recoveryEpoche)
  {
    super(dispatchId, true, validator);
    this.xid = xid;
    this.retry = retry;
    this.recoverRequestList = recoverRequestList;
    this.recoveryEpoche = recoveryEpoche;
  }

  public void setXid(XidImpl xid)
  {
    this.xid = xid;
  }

  public XidImpl getXid()
  {
    return xid;
  }

  public void setRetry(boolean retry)
  {
    this.retry = retry;
  }

  public boolean isRetry()
  {
    return retry;
  }

  public void setRecoverRequestList(List recoverRequestList)
  {
    this.recoverRequestList = recoverRequestList;
  }

  public List getRecoverRequestList()
  {
    return recoverRequestList;
  }

  public void setRecoveryEpoche(int recoveryEpoche)
  {
    this.recoveryEpoche = recoveryEpoche;
  }

  public int getRecoveryEpoche()
  {
    return recoveryEpoche;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESROLLBACK_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(xid, out);
    SMQPUtil.write(retry, out);
    if (recoverRequestList != null)
    {
      out.writeBoolean(true);
      SMQPUtil.writeRequest(recoverRequestList, out);
    } else
      out.writeBoolean(false);
    SMQPUtil.write(recoveryEpoche, out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    xid = SMQPUtil.read(xid, in);
    retry = SMQPUtil.read(retry, in);
    boolean recoverRequestList_set = in.readBoolean();
    if (recoverRequestList_set)
      recoverRequestList = SMQPUtil.readRequest(recoverRequestList, in);
    recoveryEpoche = SMQPUtil.read(recoveryEpoche, in);
  }

  protected Reply createReplyInstance()
  {
    return new XAResRollbackReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v630/XAResRollbackRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("xid=");
    _b.append(xid);
    _b.append(", ");
    _b.append("retry=");
    _b.append(retry);
    _b.append(", ");
    _b.append("recoverRequestList=");
    _b.append(recoverRequestList);
    _b.append(", ");
    _b.append("recoveryEpoche=");
    _b.append(recoveryEpoche);
    _b.append("]");
    return _b.toString();
  }
}
