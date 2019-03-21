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

/*--- formatted by Jindent 2.1, (www.c-lab.de/~jindent) ---*/

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class CloseConsumerRequest extends Request
{
  int sessionDispatchId = 0;
  int queueConsumerId = 0;
  String exception = null;

  public CloseConsumerRequest(int dispatchId, int sessionDispatchId, int queueConsumerId)
   {
     this(dispatchId,sessionDispatchId,queueConsumerId,null);
   }

  public CloseConsumerRequest(int dispatchId, int sessionDispatchId, int queueConsumerId, String exception)
   {
     super(dispatchId, true);
     this.sessionDispatchId = sessionDispatchId;
     this.queueConsumerId = queueConsumerId;
     this.exception = exception;
   }

  public int getDumpId()
  {
    return SMQPFactory.DID_CLOSE_CONSUMER_REQ;
  }

  public int getSessionDispatchId()
  {
    return sessionDispatchId;
  }

  public String getException()
  {
    return exception;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(sessionDispatchId);
    out.writeInt(queueConsumerId);
    if (exception == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      out.writeUTF(exception);
    }
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    sessionDispatchId = in.readInt();
    queueConsumerId = in.readInt();
    byte set = in.readByte();
    if (set == 1)
      exception = in.readUTF();
  }

  protected Reply createReplyInstance()
  {
    return new CloseConsumerReply();
  }

  public void setQueueConsumerId(int queueConsumerId)
  {
    this.queueConsumerId = queueConsumerId;
  }

  public int getQueueConsumerId()
  {
    return (queueConsumerId);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitCloseConsumerRequest(this);
  }

  public String toString()
  {
    return "[CloseConsumerRequest " + super.toString() + " sessionDispatchId=" + sessionDispatchId +
        " queueConsumerId=" + queueConsumerId+" exception=" + exception + "]";
  }

}



