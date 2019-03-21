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

package com.swiftmq.impl.routing.single.smqpr;

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.ReplyRequest;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProtocolReplyRequest extends ReplyRequest
{
  String protocolVersionSelected = null;

  public ProtocolReplyRequest()
  {
    super(0, false);
  }

  public int getDumpId()
  {
    return SMQRFactory.PROTOCOL_REPREQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    if (protocolVersionSelected != null)
    {
      out.writeByte(1);
      out.writeUTF(protocolVersionSelected);
    } else
      out.writeByte(0);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    byte b = in.readByte();
    if (b == 1)
      protocolVersionSelected = in.readUTF();
  }

  public String getProtocolVersionSelected()
  {
    return protocolVersionSelected;
  }

  public void setProtocolVersionSelected(String protocolVersionSelected)
  {
    this.protocolVersionSelected = protocolVersionSelected;
  }

  protected Reply createReplyInstance()
  {
    return null;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQRVisitor) visitor).handleRequest(this);
  }

  public String toString()
  {
    return "[ProtocolReplyRequest " + super.toString() + ", protocolVersionSelected=" + protocolVersionSelected + "]";
  }
}
