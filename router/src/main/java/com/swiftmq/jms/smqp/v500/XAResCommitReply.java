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

/*
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: Dec 15, 2001
 * Time: 1:04:35 PM
 * To change template for new class use
 * Code Style | Class Templates options (Tools | IDE Options).
 */
package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;

import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class XAResCommitReply extends Reply
{
  int errorCode = XAResource.XA_OK;
  long delay = 0;

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESCOMMIT_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(errorCode);
    out.writeLong(delay);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    errorCode = in.readInt();
    delay = in.readLong();
  }

  public void setErrorCode(int errorCode)
  {
    this.errorCode = errorCode;
  }

  public int getErrorCode()
  {
    return errorCode;
  }

  public void setDelay(long delay)
  {
    this.delay = delay;
  }

  public long getDelay()
  {
    return (delay);
  }

  public String toString()
  {
    return "[XAResCommitReply " + super.toString() + " errorCode=" + errorCode + " delay=" + delay + "]";
  }
}