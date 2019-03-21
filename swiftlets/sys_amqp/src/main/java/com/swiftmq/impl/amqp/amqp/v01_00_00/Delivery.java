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

package com.swiftmq.impl.amqp.amqp.v01_00_00;

import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.transport.Packager;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.MessageIndex;

public class Delivery extends Packager
{
  private SourceLink sourceLink;
  private MessageImpl message;
  private MessageIndex messageIndex;
  private AMQPMessage amqpMessage;

  public Delivery(SourceLink sourceLink, MessageImpl message, MessageIndex messageIndex)
  {
    this.sourceLink = sourceLink;
    this.message = message;
    this.messageIndex = messageIndex;
  }

  public SourceLink getSourceLink()
  {
    return sourceLink;
  }

  public MessageImpl getMessage()
  {
    return message;
  }

  public MessageIndex getMessageIndex()
  {
    return messageIndex;
  }

  public void setAmqpMessage(AMQPMessage amqpMessage)
  {
    this.amqpMessage = amqpMessage;
  }

  public AMQPMessage getAmqpMessage()
  {
    return amqpMessage;
  }
}
