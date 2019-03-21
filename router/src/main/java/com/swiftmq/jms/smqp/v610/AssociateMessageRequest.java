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

/** SMQP-Protocol Version 610, Class: AssociateMessageRequest
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

public class AssociateMessageRequest extends Request 
{
  private MessageIndex messageIndex;
  private boolean duplicateDelete;

  public AssociateMessageRequest()
  {
    super(0,true);
  }

  public AssociateMessageRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public AssociateMessageRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public AssociateMessageRequest(int dispatchId, MessageIndex messageIndex, boolean duplicateDelete)
  {
    super(dispatchId,true);
    this.messageIndex = messageIndex;
    this.duplicateDelete = duplicateDelete;
  }

  public AssociateMessageRequest(RequestRetryValidator validator, int dispatchId, MessageIndex messageIndex, boolean duplicateDelete)
  {
    super(dispatchId,true,validator);
    this.messageIndex = messageIndex;
    this.duplicateDelete = duplicateDelete;
  }
  
  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public MessageIndex getMessageIndex()
  {
    return messageIndex;
  }
  
  public void setDuplicateDelete(boolean duplicateDelete)
  {
    this.duplicateDelete = duplicateDelete;
  }

  public boolean isDuplicateDelete()
  {
    return duplicateDelete;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_ASSOCIATEMESSAGE_REQ;
  }


  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(messageIndex,out);
    SMQPUtil.write(duplicateDelete,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    messageIndex = SMQPUtil.read(messageIndex,in);
    duplicateDelete = SMQPUtil.read(duplicateDelete,in);
  }

  protected Reply createReplyInstance()
  {
    return new AssociateMessageReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v610/AssociateMessageRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("messageIndex=");
    _b.append(messageIndex);
    _b.append(", ");
    _b.append("duplicateDelete=");
    _b.append(duplicateDelete);
    _b.append("]");
    return _b.toString();
  }
}
