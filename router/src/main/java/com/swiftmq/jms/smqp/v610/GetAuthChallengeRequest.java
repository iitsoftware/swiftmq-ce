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

/** SMQP-Protocol Version 610, Class: GetAuthChallengeRequest
 *  Automatically generated, don't change!
 *  Generation Date: Mon Jul 17 17:50:10 CEST 2006
 *  (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v610.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class GetAuthChallengeRequest extends Request 
{
  private String userName;

  public GetAuthChallengeRequest()
  {
    super(0,true);
  }

  public GetAuthChallengeRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public GetAuthChallengeRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public GetAuthChallengeRequest(int dispatchId, String userName)
  {
    super(dispatchId,true);
    this.userName = userName;
  }

  public GetAuthChallengeRequest(RequestRetryValidator validator, int dispatchId, String userName)
  {
    super(dispatchId,true,validator);
    this.userName = userName;
  }
  
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getUserName()
  {
    return userName;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_GETAUTHCHALLENGE_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(userName,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    userName = SMQPUtil.read(userName,in);
  }

  protected Reply createReplyInstance()
  {
    return new GetAuthChallengeReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v610/GetAuthChallengeRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("userName=");
    _b.append(userName);
    _b.append("]");
    return _b.toString();
  }
}
