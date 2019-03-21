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

/** SMQP-Protocol Version 510, Class: XAResEndRequest
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

public class XAResEndRequest extends Request
{
  private XidImpl xid;
  private int flags;
  private List messages;

  public XAResEndRequest()
  {
    super(0,true);
  }

  public XAResEndRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public XAResEndRequest(int dispatchId, XidImpl xid, int flags, List messages)
  {
    super(dispatchId,true);
    this.xid = xid;
    this.flags = flags;
    this.messages = messages;
  }
  
  public void setXid(XidImpl xid)
  {
    this.xid = xid;
  }

  public XidImpl getXid()
  {
    return xid;
  }
  
  public void setFlags(int flags)
  {
    this.flags = flags;
  }

  public int getFlags()
  {
    return flags;
  }
  
  public void setMessages(List messages)
  {
    this.messages = messages;
  }

  public List getMessages()
  {
    return messages;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESEND_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(xid,out);
    SMQPUtil.write(flags,out);
    if (messages != null)
    {
      out.writeBoolean(true);
      SMQPUtil.writebytearray(messages,out);
    } else
      out.writeBoolean(false);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    xid = SMQPUtil.read(xid,in);
    flags = SMQPUtil.read(flags,in);
    boolean messages_set = in.readBoolean();
    if (messages_set)
      messages = SMQPUtil.readbytearray(messages,in);
  }

  protected Reply createReplyInstance()
  {
    return new XAResEndReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[XAResEndRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("xid=");
    _b.append(xid);
    _b.append(", ");
    _b.append("flags=");
    _b.append(flags);
    _b.append(", ");
    _b.append("messages=");
    _b.append(messages);
    _b.append("]");
    return _b.toString();
  }
}
