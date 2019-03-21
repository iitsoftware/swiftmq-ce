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

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import java.io.*;

/**
 * A Reply is a wrapper class for data (the Result) sending back from a communication point
 * to a Request, identified by a RequestNumber. The Reply could also be an exception. In that
 * case the ok-flag is not set and the exception contains the exception.
 * @Author Andreas Mueller, IIT GmbH
 * @Version 1.0
 */
public class Reply implements Dumpable, Serializable
{
  boolean ok = false;
  Exception exception = null;
  boolean timeout = false;
  int requestNumber = 0;
  transient ReplyHandler replyHandler = null;

  /**
   * Returns a unique dump id for this object.
   * @return unique dump id
   */
  public int getDumpId()
  {
    return (0); // NYI
  }

  /**
   * Write the content of this object to the stream.
   * @param out output stream
   * @exception IOException if an error occurs
   */
  public void writeContent(DataOutput out)
      throws IOException
  {
    out.writeBoolean(ok);
    if (exception == null)
      out.writeByte(0);
    else
    {
      out.writeByte(1);
      DataByteArrayOutputStream dos = new DataByteArrayOutputStream(256);
      (new ObjectOutputStream(dos)).writeObject(exception);
      out.writeInt(dos.getCount());
      out.write(dos.getBuffer(),0,dos.getCount());
    }
    out.writeBoolean(timeout);
    out.writeInt(requestNumber);
  }

  /**
   * Read the content of this object from the stream.
   * @param in input stream
   * @exception IOException if an error occurs
   */
  public void readContent(DataInput in)
      throws IOException
  {
    ok = in.readBoolean();
    byte set = in.readByte();
    if (set == 0)
      exception = null;
    else
    {
      try
      {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        exception = (Exception) (new ObjectInputStream(new DataByteArrayInputStream(b))).readObject();
      } catch (ClassNotFoundException ignored)
      {
      }
    }
    timeout = in.readBoolean();
    requestNumber = in.readInt();
  }

  /**
   * @SBGen Method set requestNumber
   */
  void setRequestNumber(int requestNumber)
  {
    // SBgen: Assign variable
    this.requestNumber = requestNumber;
  }

  /**
   * @SBGen Method get requestNumber
   */
  int getRequestNumber()
  {
    // SBgen: Get variable
    return (requestNumber);
  }

  /**
   * @param replyHandler
   * @SBGen Method set replyHandler
   */
  void setReplyHandler(ReplyHandler replyHandler)
  {
    // SBgen: Assign variable
    this.replyHandler = replyHandler;
  }

  /**
   * @SBGen Method set ok
   */
  public void setOk(boolean ok)
  {
    // SBgen: Assign variable
    this.ok = ok;
  }

  /**
   * @SBGen Method get ok
   */
  public boolean isOk()
  {
    // SBgen: Get variable
    return (ok);
  }

  /**
   * @SBGen Method set timeout
   */
  public void setTimeout(boolean timeout)
  {
    // SBgen: Assign variable
    this.timeout = timeout;
  }

  /**
   * @SBGen Method get timeout
   */
  public boolean isTimeout()
  {
    // SBgen: Get variable
    return (timeout);
  }

  /**
   * @SBGen Method set exception
   */
  public void setException(Exception exception)
  {
    // SBgen: Assign variable
    this.exception = exception;
  }

  /**
   * @SBGen Method get exception
   */
  public Exception getException()
  {
    // SBgen: Get variable
    return (exception);
  }

  public void send()
  {
    replyHandler.performReply(this);
  }

  public String toString()
  {
    return "[Reply, ok=" + ok + " exception=" + exception + " requestNumber=" + requestNumber + " timeout=" + timeout + " ]";
  }
}

