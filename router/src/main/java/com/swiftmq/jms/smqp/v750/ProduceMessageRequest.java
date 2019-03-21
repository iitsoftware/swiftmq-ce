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

/** SMQP-Protocol Version 750, Class: ProduceMessageRequest
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

public class ProduceMessageRequest extends Request 
{
  private int queueProducerId;
  private MessageImpl singleMessage;
  private byte[] messageCopy;

  public ProduceMessageRequest()
  {
    super(0,true);
  }

  public ProduceMessageRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public ProduceMessageRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public ProduceMessageRequest(int dispatchId, int queueProducerId, MessageImpl singleMessage, byte[] messageCopy)
  {
    super(dispatchId,true);
    this.queueProducerId = queueProducerId;
    this.singleMessage = singleMessage;
    this.messageCopy = messageCopy;
  }

  public ProduceMessageRequest(RequestRetryValidator validator, int dispatchId, int queueProducerId, MessageImpl singleMessage, byte[] messageCopy)
  {
    super(dispatchId,true,validator);
    this.queueProducerId = queueProducerId;
    this.singleMessage = singleMessage;
    this.messageCopy = messageCopy;
  }
  
  public void setQueueProducerId(int queueProducerId)
  {
    this.queueProducerId = queueProducerId;
  }

  public int getQueueProducerId()
  {
    return queueProducerId;
  }
  
  public void setSingleMessage(MessageImpl singleMessage)
  {
    this.singleMessage = singleMessage;
  }

  public MessageImpl getSingleMessage()
  {
    return singleMessage;
  }
  
  public void setMessageCopy(byte[] messageCopy)
  {
    this.messageCopy = messageCopy;
  }

  public byte[] getMessageCopy()
  {
    return messageCopy;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_PRODUCEMESSAGE_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(queueProducerId,out);
    if (singleMessage != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(singleMessage,out);
    } else
      out.writeBoolean(false);
    if (messageCopy != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(messageCopy,out);
    } else
      out.writeBoolean(false);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueProducerId = SMQPUtil.read(queueProducerId,in);
    boolean singleMessage_set = in.readBoolean();
    if (singleMessage_set)
      singleMessage = SMQPUtil.read(singleMessage,in);
    boolean messageCopy_set = in.readBoolean();
    if (messageCopy_set)
      messageCopy = SMQPUtil.read(messageCopy,in);
  }

  protected Reply createReplyInstance()
  {
    return new ProduceMessageReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/ProduceMessageRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("queueProducerId=");
    _b.append(queueProducerId);
    _b.append(", ");
    _b.append("singleMessage=");
    _b.append(singleMessage);
    _b.append(", ");
    _b.append("messageCopy=");
    _b.append(messageCopy);
    _b.append("]");
    return _b.toString();
  }
}
