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

import com.swiftmq.amqp.v091.types.Frame;
import com.swiftmq.impl.amqp.amqp.v00_09_01.AMQPChannelVisitor;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.POVisitor;

public class POChannelFrameReceived extends POObject
{
  Frame frame = null;

  public POChannelFrameReceived(Frame frame)
  {
    super(null, null);
    this.frame = frame;
  }

  public Frame getFrame()
  {
    return frame;
  }

  public void accept(POVisitor visitor)
  {
    ((AMQPChannelVisitor) visitor).visit(this);
  }

  public String toString()
  {
    return "[POChannelFrameReceived, frame=" + frame + "]";
  }
}