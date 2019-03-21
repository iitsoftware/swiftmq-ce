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

/** SMQP-Protocol Version 610, Class: CreateConsumerReply
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

public class CreateConsumerReply extends ReplyNE
{
  private int queueConsumerId;

  public CreateConsumerReply(int queueConsumerId)
  {
    this.queueConsumerId = queueConsumerId;
  }

  protected CreateConsumerReply()
  {
  }
  
  public void setQueueConsumerId(int queueConsumerId)
  {
    this.queueConsumerId = queueConsumerId;
  }

  public int getQueueConsumerId()
  {
    return queueConsumerId;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATECONSUMER_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(queueConsumerId,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueConsumerId = SMQPUtil.read(queueConsumerId,in);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v610/CreateConsumerReply, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("queueConsumerId=");
    _b.append(queueConsumerId);
    _b.append("]");
    return _b.toString();
  }
}
