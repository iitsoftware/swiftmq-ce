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

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class CreateSessionRequest extends Request
{
  public static final int QUEUE_SESSION = 0;
  public static final int TOPIC_SESSION = 1;
  public static final int UNIFIED = 2;
  boolean transacted = false;
  int acknowledgeMode = 0;
  int type = 0;

  public CreateSessionRequest(boolean transacted, int acknowledgeMode, int type)
  {
    super(0, true);
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
    this.type = type;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATE_SESSION_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeBoolean(transacted);
    out.writeInt(acknowledgeMode);
    out.writeInt(type);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    transacted = in.readBoolean();
    acknowledgeMode = in.readInt();
    type = in.readInt();
  }

  protected Reply createReplyInstance()
  {
    return new CreateSessionReply();
  }

  public void setTransacted(boolean transacted)
  {
    this.transacted = transacted;
  }

  public boolean isTransacted()
  {
    return (transacted);
  }

  public void setAcknowledgeMode(int acknowledgeMode)
  {
    this.acknowledgeMode = acknowledgeMode;
  }

  public int getAcknowledgeMode()
  {
    return (acknowledgeMode);
  }

  public int getType()
  {
    return (type);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitCreateSessionRequest(this);
  }

  public String toString()
  {
    return "[CreateSessionRequest " + super.toString() + " transacted="
        + transacted + " acknowledgeMode=" + acknowledgeMode + " type=" + type + "]";
  }

}



