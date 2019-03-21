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

package com.swiftmq.jms.smqp.v510;

/** SMQP-Protocol Version 510, Class: XAResRollbackRequest
 *  Automatically generated, don't change!
 *  Generation Date: Fri Aug 13 16:00:44 CEST 2004
 *  (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v510.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class XAResRollbackRequest extends Request
{
  private XidImpl xid;

  public XAResRollbackRequest()
  {
    super(0,true);
  }

  public XAResRollbackRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public XAResRollbackRequest(int dispatchId, XidImpl xid)
  {
    super(dispatchId,true);
    this.xid = xid;
  }
  
  public void setXid(XidImpl xid)
  {
    this.xid = xid;
  }

  public XidImpl getXid()
  {
    return xid;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESROLLBACK_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(xid,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    xid = SMQPUtil.read(xid,in);
  }

  protected Reply createReplyInstance()
  {
    return new XAResRollbackReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[XAResRollbackRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("xid=");
    _b.append(xid);
    _b.append("]");
    return _b.toString();
  }
}
