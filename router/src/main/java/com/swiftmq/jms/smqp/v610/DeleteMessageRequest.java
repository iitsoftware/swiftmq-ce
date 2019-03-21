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

/** SMQP-Protocol Version 610, Class: DeleteMessageRequest
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

public class DeleteMessageRequest extends Request 
{
  private MessageIndex messageIndex;
  private boolean fromReadTx;

  public DeleteMessageRequest()
  {
    super(0,false);
  }

  public DeleteMessageRequest(int dispatchId)
  {
    super(dispatchId,false);
  }

  public DeleteMessageRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,false,validator);
  }

  public DeleteMessageRequest(int dispatchId, MessageIndex messageIndex, boolean fromReadTx)
  {
    super(dispatchId,false);
    this.messageIndex = messageIndex;
    this.fromReadTx = fromReadTx;
  }

  public DeleteMessageRequest(RequestRetryValidator validator, int dispatchId, MessageIndex messageIndex, boolean fromReadTx)
  {
    super(dispatchId,false,validator);
    this.messageIndex = messageIndex;
    this.fromReadTx = fromReadTx;
  }
  
  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public MessageIndex getMessageIndex()
  {
    return messageIndex;
  }
  
  public void setFromReadTx(boolean fromReadTx)
  {
    this.fromReadTx = fromReadTx;
  }

  public boolean isFromReadTx()
  {
    return fromReadTx;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_DELETEMESSAGE_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(messageIndex,out);
    SMQPUtil.write(fromReadTx,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    messageIndex = SMQPUtil.read(messageIndex,in);
    fromReadTx = SMQPUtil.read(fromReadTx,in);
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v610/DeleteMessageRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("messageIndex=");
    _b.append(messageIndex);
    _b.append(", ");
    _b.append("fromReadTx=");
    _b.append(fromReadTx);
    _b.append("]");
    return _b.toString();
  }
}
