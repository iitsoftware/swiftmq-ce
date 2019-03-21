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

package com.swiftmq.jms.smqp.v630;

/** SMQP-Protocol Version 630, Class: CommitRequest
 *  Automatically generated, don't change!
 *  Generation Date: Thu Aug 30 17:17:54 CEST 2007
 *  (c) 2007, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestRetryValidator;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CommitRequest extends Request
{
  private List messages;

  public CommitRequest()
  {
    super(0, true);
  }

  public CommitRequest(int dispatchId)
  {
    super(dispatchId, true);
  }

  public CommitRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId, true, validator);
  }

  public CommitRequest(int dispatchId, List messages)
  {
    super(dispatchId, true);
    this.messages = messages;
  }

  public CommitRequest(RequestRetryValidator validator, int dispatchId, List messages)
  {
    super(dispatchId, true, validator);
    this.messages = messages;
  }

  public void setMessages(List messages)
  {
    this.messages = messages;
  }

  public List getMessages()
  {
    return messages;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_COMMIT_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (messages != null)
    {
      out.writeBoolean(true);
      SMQPUtil.writeMessageList(messages, out);
    } else
      out.writeBoolean(false);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    boolean messages_set = in.readBoolean();
    if (messages_set)
      messages = SMQPUtil.readMessageList(messages, in);
  }

  protected Reply createReplyInstance()
  {
    return new CommitReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v630/CommitRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("messages=");
    _b.append(messages);
    _b.append("]");
    return _b.toString();
  }
}
