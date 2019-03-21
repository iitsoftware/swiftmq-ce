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

package com.swiftmq.jms.smqp.v510;

/** SMQP-Protocol Version 510, Class: AsyncMessageDeliveryRequest
 *  Automatically generated, don't change!
 *  Generation Date: Fri Aug 13 16:00:44 CEST 2004
 *  (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v510.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class AsyncMessageDeliveryRequest extends Request
{
  private int listenerId;
  private MessageEntry messageEntry;
  private MessageEntry[] bulk;
  private int sessionDispatchId;
  private boolean requiresRestart;
  private int recoveryEpoche;

  public AsyncMessageDeliveryRequest()
  {
    super(0,true);
  }

  public AsyncMessageDeliveryRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public AsyncMessageDeliveryRequest(int dispatchId, int listenerId, MessageEntry messageEntry, MessageEntry[] bulk, int sessionDispatchId, boolean requiresRestart, int recoveryEpoche)
  {
    super(dispatchId,true);
    this.listenerId = listenerId;
    this.messageEntry = messageEntry;
    this.bulk = bulk;
    this.sessionDispatchId = sessionDispatchId;
    this.requiresRestart = requiresRestart;
    this.recoveryEpoche = recoveryEpoche;
  }
  
  public void setListenerId(int listenerId)
  {
    this.listenerId = listenerId;
  }

  public int getListenerId()
  {
    return listenerId;
  }
  
  public void setMessageEntry(MessageEntry messageEntry)
  {
    this.messageEntry = messageEntry;
  }

  public MessageEntry getMessageEntry()
  {
    return messageEntry;
  }
  
  public void setBulk(MessageEntry[] bulk)
  {
    this.bulk = bulk;
  }

  public MessageEntry[] getBulk()
  {
    return bulk;
  }
  
  public void setSessionDispatchId(int sessionDispatchId)
  {
    this.sessionDispatchId = sessionDispatchId;
  }

  public int getSessionDispatchId()
  {
    return sessionDispatchId;
  }
  
  public void setRequiresRestart(boolean requiresRestart)
  {
    this.requiresRestart = requiresRestart;
  }

  public boolean isRequiresRestart()
  {
    return requiresRestart;
  }
  
  public void setRecoveryEpoche(int recoveryEpoche)
  {
    this.recoveryEpoche = recoveryEpoche;
  }

  public int getRecoveryEpoche()
  {
    return recoveryEpoche;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ASYNCMESSAGEDELIVERY_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(listenerId,out);
    if (messageEntry != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(messageEntry,out);
    } else
      out.writeBoolean(false);
    if (bulk != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(bulk,out);
    } else
      out.writeBoolean(false);
    SMQPUtil.write(sessionDispatchId,out);
    SMQPUtil.write(requiresRestart,out);
    SMQPUtil.write(recoveryEpoche,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    listenerId = SMQPUtil.read(listenerId,in);
    boolean messageEntry_set = in.readBoolean();
    if (messageEntry_set)
      messageEntry = SMQPUtil.read(messageEntry,in);
    boolean bulk_set = in.readBoolean();
    if (bulk_set)
      bulk = SMQPUtil.read(bulk,in);
    sessionDispatchId = SMQPUtil.read(sessionDispatchId,in);
    requiresRestart = SMQPUtil.read(requiresRestart,in);
    recoveryEpoche = SMQPUtil.read(recoveryEpoche,in);
  }

  protected Reply createReplyInstance()
  {
    return new AsyncMessageDeliveryReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[AsyncMessageDeliveryRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("listenerId=");
    _b.append(listenerId);
    _b.append(", ");
    _b.append("messageEntry=");
    _b.append(messageEntry);
    _b.append(", ");
    _b.append("bulk=");
    _b.append(bulk);
    _b.append(", ");
    _b.append("sessionDispatchId=");
    _b.append(sessionDispatchId);
    _b.append(", ");
    _b.append("requiresRestart=");
    _b.append(requiresRestart);
    _b.append(", ");
    _b.append("recoveryEpoche=");
    _b.append(recoveryEpoche);
    _b.append("]");
    return _b.toString();
  }
}
