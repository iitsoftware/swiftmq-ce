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

package com.swiftmq.impl.amqp.amqp.v00_09_01.po;

import com.swiftmq.impl.amqp.amqp.v00_09_01.AMQPChannelVisitor;
import com.swiftmq.impl.amqp.amqp.v00_09_01.Delivery;
import com.swiftmq.impl.amqp.amqp.v00_09_01.SourceMessageProcessor;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POSendMessages extends POObject
{
  SourceMessageProcessor sourceMessageProcessor;
  int deliveryStart = 0;
  Delivery[] deliveries = null;

  public POSendMessages(SourceMessageProcessor sourceMessageProcessor)
  {
    super(null, null);
    this.sourceMessageProcessor = sourceMessageProcessor;
  }

  public int getDeliveryStart()
  {
    return deliveryStart;
  }

  public void setDeliveryStart(int deliveryStart)
  {
    this.deliveryStart = deliveryStart;
  }

  public Delivery[] getDeliveries()
  {
    return deliveries;
  }

  public void setDeliveries(Delivery[] deliveries)
  {
    this.deliveries = deliveries;
  }

  public void accept(POVisitor visitor)
  {
    ((AMQPChannelVisitor) visitor).visit(this);
  }

  public SourceMessageProcessor getSourceMessageProcessor()
  {
    return sourceMessageProcessor;
  }

  public String toString()
  {
    return "[POSendMessages, deliveryStart=" + deliveryStart + ", sourceMessageProcessor=" + sourceMessageProcessor + "]";
  }
}
