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

package com.swiftmq.jms.smqp.v400;

import com.swiftmq.tools.requestreply.Reply;

import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class XAResPrepareReply extends Reply
{
  int errorCode = XAResource.XA_OK;

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESPREPARE_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(errorCode);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    errorCode = in.readInt();
  }

  public void setErrorCode(int errorCode)
  {
    this.errorCode = errorCode;
  }

  public int getErrorCode()
  {
    return errorCode;
  }

  public String toString()
  {
    return "[XAResPrepareReply " + super.toString() + " errorCode=" + errorCode + "]";
  }
}