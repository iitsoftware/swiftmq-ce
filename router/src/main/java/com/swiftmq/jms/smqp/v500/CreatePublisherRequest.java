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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.TopicImpl;
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
public class CreatePublisherRequest extends Request
{
  TopicImpl topic = null;

  /**
   * @param topic
   * @param dispatchId
   * @SBGen Constructor assigns topic
   */
  public CreatePublisherRequest(int dispatchId, TopicImpl topic)
  {
    super(dispatchId, true);

    // SBgen: Assign variable
    this.topic = topic;
  }

  /**
   * Returns a unique dump id for this object.
   * @return unique dump id
   */
  public int getDumpId()
  {
    return SMQPFactory.DID_CREATE_PUBLISHER_REQ;
  }

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);

    if (topic == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      DestinationFactory.dumpDestination(topic, out);
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
      topic = null;
    } else
    {
      topic = (TopicImpl) DestinationFactory.createDestination(in);
    }
  }

  /**
   * @return
   */
  protected Reply createReplyInstance()
  {
    return new CreatePublisherReply();
  }

  /**
   * @param topic
   * @SBGen Method set topic
   */
  public void setTopic(TopicImpl topic)
  {

    // SBgen: Assign variable
    this.topic = topic;
  }

  /**
   * @return
   * @SBGen Method get topic
   */
  public TopicImpl getTopic()
  {

    // SBgen: Get variable
    return (topic);
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor) visitor).visitCreatePublisherRequest(this);
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
    return "[CreatePublisherRequest " + super.toString() + " topic=" + topic
        + "]";
  }

}



