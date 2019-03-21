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

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.*;

public class ProduceMessageRequest extends Request
{
  int queueProducerId;
  MessageImpl message = null;
  DataByteArrayOutputStream dos = null;
  boolean copyRequired = false;

  public ProduceMessageRequest(int dispatchId, int queueProducerId, MessageImpl message, boolean replyRequired)
  {
    this(dispatchId, queueProducerId, message, replyRequired, false);
  }

  public ProduceMessageRequest(int dispatchId, int queueProducerId, MessageImpl message, boolean replyRequired, boolean copyRequired)
  {
    super(dispatchId, replyRequired);

    this.queueProducerId = queueProducerId;
    this.copyRequired = copyRequired;
    this.message = message;
    if (message != null && copyRequired)
    {
      try
      {
        dos = new DataByteArrayOutputStream(2048);
        message.writeContent(dos);
        dos.close();
      } catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }


  public int getDumpId()
  {
    return SMQPFactory.DID_PRODUCE_MESSAGE_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);

    out.writeInt(queueProducerId);
    if (copyRequired)
      out.write(dos.getBuffer(), 0, dos.getCount());
    else
      message.writeContent(out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);

    queueProducerId = in.readInt();
    message = MessageImpl.createInstance(in.readInt());
    message.readContent(in);
  }

  protected Reply createReplyInstance()
  {
    return isReplyRequired() ? new ProduceMessageReply() : null;
  }

  public void setQueueProducerId(int queueProducerId)
  {
    this.queueProducerId = queueProducerId;
  }

  public int getQueueProducerId()
  {
    return (queueProducerId);
  }

  public MessageImpl getMessage()
  {
    return (message);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitProduceMessageRequest(this);
  }

  public String toString()
  {
    return "[ProduceMessageRequest " + super.toString() +
      " queueProducerId =" + queueProducerId +
      " copyRequired =" + copyRequired +
      " message=" + message + "]";
  }
}



