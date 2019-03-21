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

/** SMQP-Protocol Version 510, Class: CreateSessionRequest
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

public class CreateSessionRequest extends Request
{
  public static final int QUEUE_SESSION = 0;
  public static final int TOPIC_SESSION = 1;
  public static final int UNIFIED = 2;
  private boolean transacted;
  private int acknowledgeMode;
  private int type;

  public CreateSessionRequest()
  {
    super(0,true);
  }

  public CreateSessionRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public CreateSessionRequest(int dispatchId, boolean transacted, int acknowledgeMode, int type)
  {
    super(dispatchId,true);
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
    this.type = type;
  }
  
  public void setTransacted(boolean transacted)
  {
    this.transacted = transacted;
  }

  public boolean isTransacted()
  {
    return transacted;
  }
  
  public void setAcknowledgeMode(int acknowledgeMode)
  {
    this.acknowledgeMode = acknowledgeMode;
  }

  public int getAcknowledgeMode()
  {
    return acknowledgeMode;
  }
  
  public void setType(int type)
  {
    this.type = type;
  }

  public int getType()
  {
    return type;
  }

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATESESSION_REQ;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    SMQPUtil.write(transacted,out);
    SMQPUtil.write(acknowledgeMode,out);
    SMQPUtil.write(type,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    transacted = SMQPUtil.read(transacted,in);
    acknowledgeMode = SMQPUtil.read(acknowledgeMode,in);
    type = SMQPUtil.read(type,in);
  }

  protected Reply createReplyInstance()
  {
    return new CreateSessionReply();
  }

  public void accept(RequestVisitor visitor)
  {
    ((SMQPVisitor)visitor).visit(this);
  }

  public String toString()
  {
    StringBuffer _b = new StringBuffer("[CreateSessionRequest, ");
    _b.append(super.toString());
    _b.append(", ");
    _b.append("transacted=");
    _b.append(transacted);
    _b.append(", ");
    _b.append("acknowledgeMode=");
    _b.append(acknowledgeMode);
    _b.append(", ");
    _b.append("type=");
    _b.append(type);
    _b.append("]");
    return _b.toString();
  }
}
