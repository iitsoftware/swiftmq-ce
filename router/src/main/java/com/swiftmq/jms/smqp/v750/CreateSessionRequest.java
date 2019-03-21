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

/** SMQP-Protocol Version 750, Class: CreateSessionRequest
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

public class CreateSessionRequest extends Request 
{
  public static final int QUEUE_SESSION = 0;
  public static final int TOPIC_SESSION = 1;
  public static final int UNIFIED = 2;
  private boolean transacted;
  private int acknowledgeMode;
  private int type;
  private int recoveryEpoche;

  public CreateSessionRequest()
  {
    super(0,true);
  }

  public CreateSessionRequest(int dispatchId)
  {
    super(dispatchId,true);
  }

  public CreateSessionRequest(RequestRetryValidator validator, int dispatchId)
  {
    super(dispatchId,true,validator);
  }

  public CreateSessionRequest(int dispatchId, boolean transacted, int acknowledgeMode, int type, int recoveryEpoche)
  {
    super(dispatchId,true);
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
    this.type = type;
    this.recoveryEpoche = recoveryEpoche;
  }

  public CreateSessionRequest(RequestRetryValidator validator, int dispatchId, boolean transacted, int acknowledgeMode, int type, int recoveryEpoche)
  {
    super(dispatchId,true,validator);
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
    this.type = type;
    this.recoveryEpoche = recoveryEpoche;
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
  
  public void setRecoveryEpoche(int recoveryEpoche)
  {
    this.recoveryEpoche = recoveryEpoche;
  }

  public int getRecoveryEpoche()
  {
    return recoveryEpoche;
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
    SMQPUtil.write(recoveryEpoche,out);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);
    transacted = SMQPUtil.read(transacted,in);
    acknowledgeMode = SMQPUtil.read(acknowledgeMode,in);
    type = SMQPUtil.read(type,in);
    recoveryEpoche = SMQPUtil.read(recoveryEpoche,in);
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
    StringBuffer _b = new StringBuffer("[v750/CreateSessionRequest, ");
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
    _b.append(", ");
    _b.append("recoveryEpoche=");
    _b.append(recoveryEpoche);
    _b.append("]");
    return _b.toString();
  }
}
