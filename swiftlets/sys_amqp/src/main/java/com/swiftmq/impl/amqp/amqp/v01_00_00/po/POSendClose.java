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

package com.swiftmq.impl.amqp.amqp.v01_00_00.po;

import com.swiftmq.amqp.v100.generated.transport.definitions.ErrorConditionIF;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.impl.amqp.amqp.v01_00_00.AMQPConnectionVisitor;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POSendClose extends POObject
{
  ErrorConditionIF errorCondition;
  AMQPString description;

  public POSendClose(ErrorConditionIF errorCondition, AMQPString description)
  {
    super(null, null);
    this.errorCondition = errorCondition;
    this.description = description;
  }

  public ErrorConditionIF getErrorCondition()
  {
    return errorCondition;
  }

  public AMQPString getDescription()
  {
    return description;
  }

  public void accept(POVisitor visitor)
  {
    ((AMQPConnectionVisitor) visitor).visit(this);
  }

  public String toString()
  {
    return "[POSendClose, errorCondition=" + errorCondition + ", description=" + description + "]";
  }
}