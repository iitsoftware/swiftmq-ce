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

package com.swiftmq.jms.smqp.v750;

/** SMQP-Protocol Version 750, Class: XAResForgetRequest
 *  Automatically generated, don't change!
 *  Generation Date: Tue Apr 21 10:39:21 CEST 2009
 *  (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v750.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class XAResForgetRequest extends Request 
{
  private XidImpl xid;
  private boolean retry;

  public XAResForgetRequest()
  {
    super(0,true);
  }

  public XAResForgetRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public XAResForgetRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public XAResForgetRequest(int dispatchId, XidImpl xid, boolean retry)
  {
    super(dispatchId,true);
    this.xid = xid;
    this.retry = retry;
  }

  public XAResForgetRequest(RequestRetryValidator validator, int dispatchId, XidImpl xid, boolean retry)
  {
    super(dispatchId,true,validator);
    this.xid = xid;
    this.retry = retry;
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

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESFORGET_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(xid,out);
    SMQPUtil.write(retry,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    xid = SMQPUtil.read(xid,in);
    retry = SMQPUtil.read(retry,in);
  }

  protected Reply createReplyInstance()
  {
    return new XAResForgetReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/XAResForgetRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("xid=");
    _b.append(xid);
    _b.append(", ");
    _b.append("retry=");
    _b.append(retry);
    _b.append("]");
    return _b.toString();
  }
}
