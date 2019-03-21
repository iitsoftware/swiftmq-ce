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

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.swiftlet.queue.MessageIndex;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class AcknowledgeMessageRequest extends Request
{
  int queueConsumerId = -1;
  MessageIndex messageIndex = null;

  public AcknowledgeMessageRequest(int dispatchId, int queueConsumerId, MessageIndex messageIndex, boolean replyRequired)
  {
    super(dispatchId, replyRequired);
    this.queueConsumerId = queueConsumerId;
    this.messageIndex = messageIndex;
  }

  public AcknowledgeMessageRequest(int dispatchId, int queueConsumerId, MessageIndex messageIndex)
  {
    this(dispatchId,queueConsumerId,messageIndex,true);
  }

  public AcknowledgeMessageRequest(int dispatchId, MessageIndex messageIndex)
  {
    this(dispatchId,-1,messageIndex,true);
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ACKNOWLEDGE_MESSAGE_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(queueConsumerId);
    if (messageIndex == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      messageIndex.writeContent(out);
    }
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueConsumerId = in.readInt();
    byte set = in.readByte();

    if (set == 0)
    {
      messageIndex = null;
    } else
    {
      messageIndex = new MessageIndex();
      messageIndex.readContent(in);
    }
  }

  protected Reply createReplyInstance()
  {
    return new AcknowledgeMessageReply();
  }

  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public int getQueueConsumerId()
  {
    return queueConsumerId;
  }

  public void setQueueConsumerId(int queueConsumerId)
  {
    this.queueConsumerId = queueConsumerId;
  }

  public MessageIndex getMessageIndex()
  {
    return (messageIndex);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitAcknowledgeMessageRequest(this);
  }

  public String toString()
  {
    return "[AcknowledgeMessageRequest " + super.toString() + " queueConsumerId=" + queueConsumerId +
            " messageIndex=" + messageIndex + "]";
  }

}



