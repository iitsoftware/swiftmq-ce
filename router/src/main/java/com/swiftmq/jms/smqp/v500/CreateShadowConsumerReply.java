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

package com.swiftmq.jms.smqp.v500;

import com.swiftmq.tools.requestreply.Reply;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class CreateShadowConsumerReply extends Reply
{
  int queueConsumerId = 0;

  public int getDumpId()
  {
    return SMQPFactory.DID_CREATE_SHADOW_CONSUMER_REP;
  }

  public void writeContent(DataOutput out) throws IOException
  {
    super.writeContent(out);
    out.writeInt(queueConsumerId);
  }

  public void readContent(DataInput in) throws IOException
  {
    super.readContent(in);

    queueConsumerId = in.readInt();
  }

  public void setQueueConsumerId(int queueConsumerId)
  {
    this.queueConsumerId = queueConsumerId;
  }

  public int getQueueConsumerId()
  {
    return (queueConsumerId);
  }

  public String toString()
  {
    return "[CreateShadowConsumerReply " + super.toString() + " queueConsumerId="
        + queueConsumerId + "]";
  }

}



