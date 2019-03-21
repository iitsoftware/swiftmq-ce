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

package com.swiftmq.tools.requestreply;

import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.*;

/**
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class GenericReply extends Reply
{
  Serializable result = null;

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out)
      throws IOException
  {
    super.writeContent(out);
    if (result == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      DataByteArrayOutputStream dos = new DataByteArrayOutputStream(256);
      (new ObjectOutputStream(dos)).writeObject(result);
      out.writeInt(dos.getCount());
      out.write(dos.getBuffer(),0,dos.getCount());
    }
  }

  /**
   * Read the content of this object from the stream.
   * @param in input stream
   * @exception IOException if an error occurs
   */
  public void readContent(DataInput in)
      throws IOException
  {
    super.readContent(in);
    byte set = in.readByte();
    if (set == 0)
      result = null;
    else
    {
      try
      {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        result = (Serializable) (new ObjectInputStream(new DataByteArrayInputStream(b))).readObject();
      } catch (ClassNotFoundException ignored)
      {
      }
    }
  }

  /**
   * @param result
   * @SBGen Method set result
   */
  public void setResult(Serializable result)
  {
    // SBgen: Assign variable
    this.result = result;
  }

  /**
   * @return
   * @SBGen Method get result
   */
  public Serializable getResult()
  {
    // SBgen: Get variable
    return (result);
  }

  public String toString()
  {
    return "[GenericReply " + super.toString() + " result=" + result + "]";
  }
}

