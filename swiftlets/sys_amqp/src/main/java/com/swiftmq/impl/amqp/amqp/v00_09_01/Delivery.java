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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.amqp.v091.types.ContentHeaderProperties;
import com.swiftmq.swiftlet.queue.MessageIndex;

public class Delivery
{
  boolean redelivered;
  MessageIndex messageIndex = null;
  ContentHeaderProperties contentHeaderProperties = null;
  byte[] body = null;

  public Delivery(ContentHeaderProperties contentHeaderProperties, byte[] body)
  {
    this.contentHeaderProperties = contentHeaderProperties;
    this.body = body;
  }

  public void setRedelivered(boolean redelivered)
  {
    this.redelivered = redelivered;
  }

  public boolean isRedelivered()
  {
    return redelivered;
  }

  public MessageIndex getMessageIndex()
  {
    return messageIndex;
  }

  public void setMessageIndex(MessageIndex messageIndex)
  {
    this.messageIndex = messageIndex;
  }

  public ContentHeaderProperties getContentHeaderProperties()
  {
    return contentHeaderProperties;
  }

  public byte[] getBody()
  {
    return body;
  }

  public String toString()
  {
    final StringBuffer sb = new StringBuffer();
    sb.append("[Delivery");
    sb.append(", redelivered=").append(redelivered);
    sb.append(", messageIndex=").append(messageIndex);
    sb.append(", contentHeaderProperties=").append(contentHeaderProperties);
    sb.append(", body.length=").append(body == null ? "null" : body.length);
    sb.append(']');
    return sb.toString();
  }
}
