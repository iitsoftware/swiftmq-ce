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

package com.swiftmq.impl.routing.single.smqpr.v942;

import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RollbackRequest extends Request
{
  XidImpl xid = null;

  RollbackRequest()
  {
    this(null);
  }

  public RollbackRequest(XidImpl xid)
  {
    super(0, false);
    this.xid = xid;
  }

  public int getDumpId()
  {
    return SMQRFactory.ROLLBACK_REQ;
  }

  public void writeContent(DataOutput output) throws IOException
  {
    super.writeContent(output);
    xid.writeContent(output);
  }

  public void readContent(DataInput input) throws IOException
  {
    super.readContent(input);
    xid = new XidImpl();
    xid.readContent(input);
  }

  public XidImpl getXid()
  {
    return xid;
  }

  public void setXid(XidImpl xid)
  {
    this.xid = xid;
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQRVisitor) visitor).handleRequest(this);
  }

  public String toString()
  {
    return "[RollbackRequest " + super.toString() + ", xid=" + xid + "]";
  }
}
