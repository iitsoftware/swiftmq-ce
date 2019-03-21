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

import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestVisitor;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class CommitRequest extends Request
{
  Object[] messages;

  /**
   * @param dispatchId
   * @SBGen Constructor
   */
  public CommitRequest(int dispatchId)
  {
    super(dispatchId, true);
  }

  /**
   * Returns a unique dump id for this object.
   * @return unique dump id
   */
  public int getDumpId()
  {
    return SMQPFactory.DID_COMMIT_REQ;
  }

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);

    if (messages == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      out.writeInt(messages.length);
      for (int i = 0; i < messages.length; i++)
      {
        byte[] msg = (byte[]) messages[i];
        out.writeInt(msg.length);
        out.write(msg);
      }
    }
  }

  /**
   * Read the content of this object from the stream.
   * @param in input stream
   * @exception IOException if an error occurs
   */
  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);

    byte set = in.readByte();

    if (set == 0)
    {
      messages = null;
    } else
    {
      int len = in.readInt();

      messages = new Object[len];

      for (int i = 0; i < len; i++)
      {
        int msglen = in.readInt();
        byte[] arr = new byte[msglen];
        in.readFully(arr);
        messages[i] = arr;
      }
    }
  }

  /**
   * Method declaration
   *
   *
   * @return
   *
   * @see
   */
  protected Reply createReplyInstance()
  {
    return new CommitReply();
  }

  /**
   * @param messages
   * @SBGen Method set messages
   */
  public void setMessages(Object[] messages)
  {

    // SBgen: Assign variable
    this.messages = messages;
  }

  /**
   * @return
   * @SBGen Method get messages
   */
  public Object[] getMessages()
  {

    // SBgen: Get variable
    return (messages);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitCommitRequest(this);
  }

  /**
   * Method declaration
   *
   *
   * @return
   *
   * @see
   */
  public String toString()
  {
    return "[CommitRequest " + super.toString() + " messages=" + messages
        + "]";
  }

}



