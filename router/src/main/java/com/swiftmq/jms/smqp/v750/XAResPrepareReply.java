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

package com.swiftmq.jms.smqp.v750;

/** SMQP-Protocol Version 750, Class: XAResPrepareReply
 *  Automatically generated, don't change!
 *  Generation Date: Tue Apr 21 10:39:21 CEST 2009
 *  (c) 2009, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v750.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class XAResPrepareReply extends ReplyNE
{
  private int errorCode;

  public XAResPrepareReply(int errorCode)
  {
    this.errorCode = errorCode;
  }

  protected XAResPrepareReply()
  {
  }
  
  public void setErrorCode(int errorCode)
  {
    this.errorCode = errorCode;
  }

  public int getErrorCode()
  {
    return errorCode;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESPREPARE_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(errorCode,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    errorCode = SMQPUtil.read(errorCode,in);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v750/XAResPrepareReply, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("errorCode=");
    _b.append(errorCode);
    _b.append("]");
    return _b.toString();
  }
}
