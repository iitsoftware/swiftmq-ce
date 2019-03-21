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

package com.swiftmq.jms.smqp.v600;

/** SMQP-Protocol Version 600, Class: ProduceMessageReply
 *  Automatically generated, don't change!
 *  Generation Date: Thu Feb 09 09:59:46 CET 2006
 *  (c) 2006, IIT GmbH, Bremen/Germany, All Rights Reserved
 **/

import com.swiftmq.tools.requestreply.ReplyNE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProduceMessageReply extends ReplyNE
{
  private long delay;

  public ProduceMessageReply(long delay)
  {
    this.delay = delay;
  }

  protected ProduceMessageReply()
  {
  }

  public void setDelay(long delay)
  {
    this.delay = delay;
  }

  public long getDelay()
  {
    return delay;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_PRODUCEMESSAGE_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(delay, out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    delay = SMQPUtil.read(delay, in);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[v600/ProduceMessageReply, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("delay=");
    _b.append(delay);
    _b.append("]");
    return _b.toString();
  }
}
