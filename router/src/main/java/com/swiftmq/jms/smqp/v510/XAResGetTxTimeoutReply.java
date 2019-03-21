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

package com.swiftmq.jms.smqp.v510;

/** SMQP-Protocol Version 510, Class: XAResGetTxTimeoutReply
 *  Automatically generated, don't change!
 *  Generation Date: Fri Aug 13 16:00:44 CEST 2004
 *  (c) 2004, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.jms.*;
import com.swiftmq.jms.v510.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.requestreply.*;
import java.io.*;
import java.util.*;
import javax.jms.*;

public class XAResGetTxTimeoutReply extends ReplyNE
{
  private long txTimeout;

  public XAResGetTxTimeoutReply(long txTimeout)
  {
    this.txTimeout = txTimeout;
  }

  protected XAResGetTxTimeoutReply()
  {
  }
  
  public void setTxTimeout(long txTimeout)
  {
    this.txTimeout = txTimeout;
  }

  public long getTxTimeout()
  {
    return txTimeout;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_XARESGETTXTIMEOUT_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(txTimeout,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    txTimeout = SMQPUtil.read(txTimeout,in);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[XAResGetTxTimeoutReply, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("txTimeout=");
    _b.append(txTimeout);
    _b.append("]");
    return _b.toString();
  }
}
