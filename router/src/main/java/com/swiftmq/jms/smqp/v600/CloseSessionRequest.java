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

package com.swiftmq.jms.smqp.v600;

/** SMQP-Protocol Version 600, Class: CloseSessionRequest
 *  Automatically generated, don't change!
 *  Generation Date: Thu Feb 09 09:59:46 CET 2006
 *  (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CloseSessionRequest extends Request
{
  private int sessionDispatchId;

  public CloseSessionRequest()
  {
    super(0, true);
  }

  public CloseSessionRequest(int dispatchId)
  {
    super(dispatchId, true);
  }

  public CloseSessionRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId, true, validator);
  }

  public CloseSessionRequest(int dispatchId, int sessionDispatchId)
  {
    super(dispatchId, true);
    this.sessionDispatchId = sessionDispatchId;
  }

  public CloseSessionRequest(RequestRetryValidator validator, int dispatchId, int sessionDispatchId)
  {
    super(dispatchId, true, validator);
    this.sessionDispatchId = sessionDispatchId;
  }

  public void setSessionDispatchId(int sessionDispatchId)
  {
    this.sessionDispatchId = sessionDispatchId;
  }

  public int getSessionDispatchId()
  {
    return sessionDispatchId;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CLOSESESSION_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(sessionDispatchId, out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    sessionDispatchId = SMQPUtil.read(sessionDispatchId, in);
  }

  protected Reply createReplyInstance()
  {
    return new CloseSessionReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v600/CloseSessionRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("sessionDispatchId=");
    _b.append(sessionDispatchId);
    _b.append("]");
    return _b.toString();
  }
}
