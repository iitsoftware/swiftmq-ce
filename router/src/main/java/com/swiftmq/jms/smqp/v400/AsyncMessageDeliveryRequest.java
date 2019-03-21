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

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class AsyncMessageDeliveryRequest extends Request
{
  int listenerId = 0;
  MessageEntry messageEntry = null;
  MessageEntry[] bulk = null;
  int sessionDispatchId = 0;
  boolean requiresRestart = false;
  int recoveryEpoche = 0;

  public AsyncMessageDeliveryRequest(int dispatchId, int listenerId,
                                     MessageEntry messageEntry, int sessionDispatchId)
  {
    super(dispatchId, false);

    this.listenerId = listenerId;
    this.messageEntry = messageEntry;
    this.sessionDispatchId = sessionDispatchId;
  }

  public AsyncMessageDeliveryRequest(int dispatchId, int listenerId,
                                     MessageEntry[] bulk, int numberMessages, int sessionDispatchId)
  {
    super(dispatchId, false);

    this.listenerId = listenerId;
    this.sessionDispatchId = sessionDispatchId;
    this.bulk = new MessageEntry[numberMessages];
    System.arraycopy(bulk, 0, this.bulk, 0, numberMessages);
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ASYNC_MESSAGE_DELIVERY_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(listenerId);
    out.writeInt(sessionDispatchId);
    out.writeInt(recoveryEpoche);
    out.writeBoolean(requiresRestart);

    if (messageEntry == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      messageEntry.writeContent(out);
    }
    if (bulk == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      out.writeInt(bulk.length);
      for (int i = 0; i < bulk.length; i++)
        bulk[i].writeContent(out);
    }
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);

    listenerId = in.readInt();
    sessionDispatchId = in.readInt();
    recoveryEpoche = in.readInt();
    requiresRestart = in.readBoolean();

    byte set = in.readByte();

    if (set == 0)
    {
      messageEntry = null;
    } else
    {
      messageEntry = new MessageEntry();
      messageEntry.readContent(in);
    }

    set = in.readByte();
    if (set == 0)
    {
      bulk = null;
    } else
    {
      bulk = new MessageEntry[in.readInt()];
      for (int i = 0; i < bulk.length; i++)
      {
        MessageEntry entry = new MessageEntry();
        entry.readContent(in);
        bulk[i] = entry;
      }
    }
  }

  protected Reply createReplyInstance()
  {
    return isReplyRequired() ? new AsyncMessageDeliveryReply(sessionDispatchId) : null;
  }

  public void setListenerId(int listenerId)
  {
    this.listenerId = listenerId;
  }

  public int getListenerId()
  {
    return (listenerId);
  }

  public boolean isRequiresRestart()
  {
    return requiresRestart;
  }

  public void setRequiresRestart(boolean requiresRestart)
  {
    this.requiresRestart = requiresRestart;
  }

  public void setMessageEntry(MessageEntry messageEntry)
  {
    this.messageEntry = messageEntry;
  }

  public boolean isBulk()
  {
    return bulk != null;
  }

  public int getRecoveryEpoche()
  {
    return recoveryEpoche;
  }

  public void setRecoveryEpoche(int recoveryEpoche)
  {
    this.recoveryEpoche = recoveryEpoche;
  }

  public AsyncMessageDeliveryRequest[] createRequests()
  {
    AsyncMessageDeliveryRequest[] requests = new AsyncMessageDeliveryRequest[bulk.length];
    for (int i = 0; i < bulk.length; i++)
    {
      requests[i] = new AsyncMessageDeliveryRequest(-1, listenerId, bulk[i], sessionDispatchId);
      requests[i].setRecoveryEpoche(recoveryEpoche);
    }
    return requests;
  }

  public MessageEntry getMessageEntry()
  {
    return (messageEntry);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitAsyncMessageDeliveryRequest(this);
  }

  public String toString()
  {
    return "[AsyncMessageDeliveryRequest " + super.toString()
        + " listenerId=" + listenerId + " sessionDispatchId=" + sessionDispatchId + " recoveryEpoche=" + recoveryEpoche
        + " requiresRestart=" + requiresRestart + " messageEntry=" + messageEntry + " bulk=" + bulk + "]";
  }

}



