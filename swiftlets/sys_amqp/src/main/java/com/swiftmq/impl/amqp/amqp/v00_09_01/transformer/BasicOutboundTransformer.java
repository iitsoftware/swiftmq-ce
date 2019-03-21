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

package com.swiftmq.impl.amqp.amqp.v00_09_01.transformer;

import com.swiftmq.amqp.v091.types.ContentHeaderProperties;
import com.swiftmq.amqp.v091.types.Field;
import com.swiftmq.impl.amqp.amqp.v00_09_01.Delivery;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class BasicOutboundTransformer extends OutboundTransformer
{
  public BasicOutboundTransformer()
  {
    try
    {
      setConfiguration(new HashMap());
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private char toFieldType(Object obj)
  {
    if (obj == null)
      return 'V';
    if (obj instanceof String)
      return 'S';
    if (obj instanceof Long)
      return 'T';
    if (obj instanceof Integer)
      return 'I';
    if (obj instanceof Short)
      return 's';
    if (obj instanceof Float)
      return 'f';
    if (obj instanceof Boolean)
      return 't';
    return 'V';
  }

  public Delivery transform(MessageImpl jmsMessage) throws Exception
  {
    jmsMessage.reset();
    ContentHeaderProperties prop = new ContentHeaderProperties();
    byte[] body = null;
    if (jmsMessage instanceof TextMessageImpl)
    {
      String text = ((TextMessageImpl) jmsMessage).getText();
      if (text != null)
        body = text.getBytes("utf-8");
    } else if (jmsMessage instanceof BytesMessageImpl)
    {
      BytesMessageImpl bytesMessage = (BytesMessageImpl) jmsMessage;
      if (bytesMessage.getBodyLength() > 0)
      {
        body = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(body);
      }
    } else
      throw new Exception("Unable to tranform (only Text/BytesMessages supported): " + jmsMessage);

    // Fill props
    prop.setWeight(0);
    prop.setBodySize(Long.valueOf(body.length));
    prop.setContentType(jmsMessage.getStringProperty(prefixVendor + "ContentType"));
    prop.setContentEncoding(jmsMessage.getStringProperty(prefixVendor + "ContentEncoding"));
    prop.setDeliveryMode(jmsMessage.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT ? 1 : 2);
    prop.setPriority(jmsMessage.getJMSPriority());
    prop.setCorrelationId(jmsMessage.getJMSCorrelationID());
    Destination replyTo = jmsMessage.getJMSReplyTo();
    if (replyTo != null)
      prop.setReplyTo(replyTo.toString());
    prop.setExpiration(String.valueOf(jmsMessage.getJMSExpiration()));
    prop.setMessageId(jmsMessage.getJMSMessageID());
    prop.setTimestamp(jmsMessage.getJMSTimestamp());
    prop.setType(jmsMessage.getStringProperty(prefixVendor + "Type"));
    prop.setUserId(jmsMessage.getStringProperty(prefixVendor + "UserId"));
    prop.setAppId(jmsMessage.getStringProperty(prefixVendor + "AppId"));
    prop.setClusterId(jmsMessage.getStringProperty(prefixVendor + "ClusterId"));
    Map<String, Object> headers = new HashMap<String, Object>();
    for (Enumeration _enum = jmsMessage.getPropertyNames(); _enum.hasMoreElements(); )
    {
      String name = (String) _enum.nextElement();
      if (!name.startsWith(prefixVendor))
      {
        Object value = jmsMessage.getObjectProperty(name);
        char type = toFieldType(value);
        if (value instanceof String)
          value = ((String) value).getBytes("utf-8");
        headers.put(nameTranslator.translate(name), new Field(type, value));
      }
    }
    if (headers.size() > 0)
      prop.setHeaders(headers);
    return new Delivery(prop, body);
  }
}
