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
public class FetchBrowserMessageRequest extends Request
{
  int queueBrowserId = 0;
  boolean resetRequired = false;

  /**
   * @param queueBrowserId
   * @param dispatchId
   * @SBGen Constructor assigns queueBrowserId
   */
  public FetchBrowserMessageRequest(int dispatchId, int queueBrowserId, boolean resetRequired)
  {
    super(dispatchId, true);

    // SBgen: Assign variable
    this.queueBrowserId = queueBrowserId;
    this.resetRequired = resetRequired;
  }

  /**
   * Returns a unique dump id for this object.
   * @return unique dump id
   */
  public int getDumpId()
  {
    return SMQPFactory.DID_FETCH_BROWSER_MESSAGE_REQ;
  }

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(queueBrowserId);
    out.writeBoolean(resetRequired);
  }

  /**
   * Read the content of this object from the stream.
   * @param in input stream
   * @exception IOException if an error occurs
   */
  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    queueBrowserId = in.readInt();
    resetRequired = in.readBoolean();
  }

  /**
   * @return
   */
  protected Reply createReplyInstance()
  {
    return new FetchBrowserMessageReply();
  }

  /**
   * @param queueBrowserId
   * @SBGen Method set queueBrowserId
   */
  public void setQueueBrowserId(int queueBrowserId)
  {

    // SBgen: Assign variable
    this.queueBrowserId = queueBrowserId;
  }

  /**
   * @return
   * @SBGen Method get queueBrowserId
   */
  public int getQueueBrowserId()
  {

    // SBgen: Get variable
    return (queueBrowserId);
  }

  public boolean isResetRequired()
  {
    return resetRequired;
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitFetchBrowserMessageRequest(this);
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
    return "[FetchBrowserMessageRequest " + super.toString()
        + " queueBrowserId=" + queueBrowserId + ", resetRequired=" + resetRequired + "]";
  }

}



