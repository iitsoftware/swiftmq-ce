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
import com.swiftmq.jms.v400.ConnectionMetaDataImpl;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class GetMetaDataReply extends Reply
{
  ConnectionMetaDataImpl connectionMetaData = null;

  /**
   * Returns a unique dump id for this object.
   * @return unique dump id
   */
  public int getDumpId()
  {
    return SMQPFactory.DID_GET_META_DATA_REP;
  }

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);

    if (connectionMetaData == null)
    {
      out.writeByte(0);
    } else
    {
      out.writeByte(1);
      connectionMetaData.writeContent(out);
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
      connectionMetaData = null;
    } else
    {
      connectionMetaData = new ConnectionMetaDataImpl();

      connectionMetaData.readContent(in);
    }
  }

  /**
   * @param connectionMetaData
   * @SBGen Method set connectionMetaData
   */
  public void setConnectionMetaData(ConnectionMetaDataImpl connectionMetaData)
  {

    // SBgen: Assign variable
    this.connectionMetaData = connectionMetaData;
  }

  /**
   * @return
   * @SBGen Method get connectionMetaData
   */
  public ConnectionMetaDataImpl getConnectionMetaData()
  {

    // SBgen: Get variable
    return (connectionMetaData);
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
    return "[GetMetaDataReply " + super.toString() + " connectionMetaData="
        + connectionMetaData + "]";
  }

}



