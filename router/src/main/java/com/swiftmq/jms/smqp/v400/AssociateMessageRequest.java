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

public class AssociateMessageRequest extends Request
{
  MessageIndex messageIndex = null;

  public AssociateMessageRequest(int dispatchId, MessageIndex messageIndex)
  {
    super(dispatchId, true);
    this.messageIndex = messageIndex;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ASSOCIATE_MESSAGE_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);

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
    return new AssociateMessageReply();
  }

  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public MessageIndex getMessageIndex()
  {
    return (messageIndex);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitAssociateMessageRequest(this);
  }

  public String toString()
  {
    return "[AssociateMessageRequest " + super.toString() + " messageIndex="
        + messageIndex + "]";
  }

}




