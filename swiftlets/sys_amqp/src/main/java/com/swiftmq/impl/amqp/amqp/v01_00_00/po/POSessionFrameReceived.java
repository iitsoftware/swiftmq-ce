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

import com.swiftmq.amqp.v100.generated.transport.performatives.FrameIF;
import com.swiftmq.impl.amqp.amqp.v01_00_00.AMQPSessionVisitor;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POSessionFrameReceived extends POObject
{
  FrameIF frame = null;

  public POSessionFrameReceived(FrameIF frame)
  {
    super(null, null);
    this.frame = frame;
  }

  public FrameIF getFrame()
  {
    return frame;
  }

  public void accept(POVisitor visitor)
  {
    ((AMQPSessionVisitor) visitor).visit(this);
  }

  public String toString()
  {
    return "[POSessionFrameReceived, frame=" + frame + "]";
  }
}