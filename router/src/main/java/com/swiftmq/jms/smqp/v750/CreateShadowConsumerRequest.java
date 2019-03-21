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

/** SMQP-Protocol Version 750, Class: CreateShadowConsumerRequest
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

public class CreateShadowConsumerRequest extends Request 
{
  private String queueName;

  public CreateShadowConsumerRequest()
  {
    super(0,true);
  }

  public CreateShadowConsumerRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public CreateShadowConsumerRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public CreateShadowConsumerRequest(int dispatchId, String queueName)
  {
    super(dispatchId,true);
    this.queueName = queueName;
  }

  public CreateShadowConsumerRequest(RequestRetryValidator validator, int dispatchId, String queueName)
  {
    super(dispatchId,true,validator);
    this.queueName = queueName;
  }
  
  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATESHADOWCONSUMER_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(queueName,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueName = SMQPUtil.read(queueName,in);
  }

  protected Reply createReplyInstance()
  {
    return new CreateShadowConsumerReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/CreateShadowConsumerRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("queueName=");
    _b.append(queueName);
    _b.append("]");
    return _b.toString();
  }
}
