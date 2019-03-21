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

/** SMQP-Protocol Version 750, Class: FetchBrowserMessageRequest
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

public class FetchBrowserMessageRequest extends Request 
{
  private int queueBrowserId;
  private boolean resetRequired;
  private MessageIndex lastMessageIndex;

  public FetchBrowserMessageRequest()
  {
    super(0,true);
  }

  public FetchBrowserMessageRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public FetchBrowserMessageRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public FetchBrowserMessageRequest(int dispatchId, int queueBrowserId, boolean resetRequired, MessageIndex lastMessageIndex)
  {
    super(dispatchId,true);
    this.queueBrowserId = queueBrowserId;
    this.resetRequired = resetRequired;
    this.lastMessageIndex = lastMessageIndex;
  }

  public FetchBrowserMessageRequest(RequestRetryValidator validator, int dispatchId, int queueBrowserId, boolean resetRequired, MessageIndex lastMessageIndex)
  {
    super(dispatchId,true,validator);
    this.queueBrowserId = queueBrowserId;
    this.resetRequired = resetRequired;
    this.lastMessageIndex = lastMessageIndex;
  }
  
  public void setQueueBrowserId(int queueBrowserId)
  {
    this.queueBrowserId = queueBrowserId;
  }

  public int getQueueBrowserId()
  {
    return queueBrowserId;
  }
  
  public void setResetRequired(boolean resetRequired)
  {
    this.resetRequired = resetRequired;
  }

  public boolean isResetRequired()
  {
    return resetRequired;
  }
  
  public void setLastMessageIndex(MessageIndex lastMessageIndex)
  {
    this.lastMessageIndex = lastMessageIndex;
  }

  public MessageIndex getLastMessageIndex()
  {
    return lastMessageIndex;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_FETCHBROWSERMESSAGE_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(queueBrowserId,out);
    SMQPUtil.write(resetRequired,out);
    if (lastMessageIndex != null)
    {
      out.writeBoolean(true);
      SMQPUtil.write(lastMessageIndex,out);
    } else
      out.writeBoolean(false);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueBrowserId = SMQPUtil.read(queueBrowserId,in);
    resetRequired = SMQPUtil.read(resetRequired,in);
    boolean lastMessageIndex_set = in.readBoolean();
    if (lastMessageIndex_set)
      lastMessageIndex = SMQPUtil.read(lastMessageIndex,in);
  }

  protected Reply createReplyInstance()
  {
    return new FetchBrowserMessageReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/FetchBrowserMessageRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("queueBrowserId=");
    _b.append(queueBrowserId);
    _b.append(", ");
    _b.append("resetRequired=");
    _b.append(resetRequired);
    _b.append(", ");
    _b.append("lastMessageIndex=");
    _b.append(lastMessageIndex);
    _b.append("]");
    return _b.toString();
  }
}
