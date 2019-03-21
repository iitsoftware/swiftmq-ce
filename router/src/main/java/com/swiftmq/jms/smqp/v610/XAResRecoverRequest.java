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

package com.swiftmq.jms.smqp.v610;

/** SMQP-Protocol Version 610, Class: XAResRecoverRequest
 *  Automatically generated, don't change!
 *  Generation Date: Mon Jul 17 17:50:11 CEST 2006
 *  (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v610.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class XAResRecoverRequest extends Request 
{
  private int flags;

  public XAResRecoverRequest()
  {
    super(0,true);
  }

  public XAResRecoverRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public XAResRecoverRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public XAResRecoverRequest(int dispatchId, int flags)
  {
    super(dispatchId,true);
    this.flags = flags;
  }

  public XAResRecoverRequest(RequestRetryValidator validator, int dispatchId, int flags)
  {
    super(dispatchId,true,validator);
    this.flags = flags;
  }
  
  public void setFlags(int flags)
  {
    this.flags = flags;
  }

  public int getFlags()
  {
    return flags;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESRECOVER_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(flags,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    flags = SMQPUtil.read(flags,in);
  }

  protected Reply createReplyInstance()
  {
    return new XAResRecoverReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v610/XAResRecoverRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("flags=");
    _b.append(flags);
    _b.append("]");
    return _b.toString();
  }
}
