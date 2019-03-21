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

import com.swiftmq.amqp.v091.generated.basic.Publish;
import com.swiftmq.amqp.v091.types.ContentHeaderProperties;

import java.util.ArrayList;
import java.util.List;

public class MessageWrap
{
  Publish publish = null;
  ContentHeaderProperties contentHeaderProperties = null;
  List<byte[]> bodyParts = new ArrayList<byte[]>();
  int currentSize = 0;

  public MessageWrap(Publish publish)
  {
    this.publish = publish;
  }

  public Publish getPublish()
  {
    return publish;
  }

  public void setContentHeaderProperties(ContentHeaderProperties contentHeaderProperties)
  {
    this.contentHeaderProperties = contentHeaderProperties;
  }

  public ContentHeaderProperties getContentHeaderProperties()
  {
    return contentHeaderProperties;
  }

  public boolean addBodyPart(byte[] part)
  {
    currentSize += part.length;
    bodyParts.add(part);
    return currentSize >= contentHeaderProperties.getBodySize();
  }

  public List<byte[]> getBodyParts()
  {
    return bodyParts;
  }

  public String toString()
  {
    final StringBuffer sb = new StringBuffer();
    sb.append("[MessageWrap");
    sb.append(" publish=").append(publish);
    sb.append(", contentHeaderProperties=").append(contentHeaderProperties);
    sb.append(", bodyParts=").append(bodyParts);
    sb.append(", currentSize=").append(currentSize);
    sb.append(']');
    return sb.toString();
  }
}
