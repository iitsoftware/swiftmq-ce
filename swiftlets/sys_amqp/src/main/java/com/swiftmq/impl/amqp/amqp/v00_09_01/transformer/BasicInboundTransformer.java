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
import com.swiftmq.impl.amqp.amqp.v00_09_01.MessageWrap;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TextMessageImpl;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicInboundTransformer extends InboundTransformer {
    String messageFormat;

    public BasicInboundTransformer() {
        try {
            setConfiguration(new HashMap());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void transformHeader(MessageImpl msg, ContentHeaderProperties prop, DestinationFactory destinationFactory) throws JMSException {
        if (prop.getContentType() != null)
            msg.setStringProperty(prefixVendor + "ContentType", prop.getContentType());
        if (prop.getContentEncoding() != null)
            msg.setStringProperty(prefixVendor + "ContentEncoding", prop.getContentEncoding());
        if (prop.getDeliveryMode() != null)
            msg.setJMSDeliveryMode(prop.getDeliveryMode() == 1 ? DeliveryMode.NON_PERSISTENT : DeliveryMode.PERSISTENT);
        else
            msg.setJMSDeliveryMode(defaultDeliveryMode);
        if (prop.getPriority() != null)
            msg.setJMSPriority(prop.getPriority());
        else
            msg.setJMSPriority(defaultPriority);
        if (prop.getCorrelationId() != null)
            msg.setJMSCorrelationID(prop.getCorrelationId());
        if (prop.getReplyTo() != null)
            msg.setJMSReplyTo(destinationFactory.create(prop.getReplyTo()));
        if (prop.getExpiration() != null) {
            long value = Long.valueOf(prop.getExpiration());
            if (value > 0)
                msg.setJMSExpiration(System.currentTimeMillis() + value);
        }
        if (prop.getMessageId() != null)
            msg.setJMSMessageID(prop.getMessageId());
        else
            msg.setJMSMessageID(nextMsgId());
        if (prop.getTimestamp() != null)
            msg.setJMSTimestamp(prop.getTimestamp());
        else
            msg.setJMSTimestamp(System.currentTimeMillis());
        if (prop.getType() != null)
            msg.setStringProperty(prefixVendor + "Type", prop.getType());
        if (prop.getUserId() != null)
            msg.setStringProperty(prefixVendor + "UserId", prop.getUserId());
        if (prop.getAppId() != null)
            msg.setStringProperty(prefixVendor + "AppId", prop.getAppId());
        if (prop.getClusterId() != null)
            msg.setStringProperty(prefixVendor + "ClusterId", prop.getClusterId());

        Map<String, Object> headers = prop.getHeaders();
        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                String name = nameTranslator.translate((String) entry.getKey());
                Field field = (Field) entry.getValue();
                switch (field.getType()) {
                    case 't':
                        msg.setBooleanProperty(name, (Boolean) field.getValue());
                        break;
                    case 'b':
                    case 'B':
                    case 'U':
                    case 'u':
                    case 'I':
                        msg.setIntProperty(name, (Integer) field.getValue());
                        break;
                    case 'i':
                    case 'L':
                        msg.setLongProperty(name, (Long) field.getValue());
                        break;
                    case 'l':
                        break;
                    case 'f':
                    case 'd':
                        msg.setFloatProperty(name, (Float) field.getValue());
                        break;
                    case 'D':
                        break;
                    case 's':
                        msg.setShortProperty(name, (Short) field.getValue());
                        break;
                    case 'S':
                        msg.setStringProperty(name, (String) field.getStringValue());
                        break;
                    case 'A':
                        break;
                    case 'T':
                        msg.setLongProperty(name, (Long) field.getValue());
                        break;
                    case 'F':
                        break;
                    case 'V':
                        break;
                }
            }
        }
    }

    public void setConfiguration(Map config) throws Exception {
        super.setConfiguration(config);
        messageFormat = prefixVendor + Util.PROP_MESSAGE_FORMAT;
    }

    private byte[] getPayload(int len, List<byte[]> bodyParts) {
        byte[] buffer = new byte[len];
        int pos = 0;
        for (int i = 0; i < bodyParts.size(); i++) {
            byte[] b = bodyParts.get(i);
            System.arraycopy(b, 0, buffer, pos, b.length);
            pos += b.length;
        }
        return buffer;
    }

    public MessageImpl transform(MessageWrap messageWrap, DestinationFactory destinationFactory) throws JMSException {
        MessageImpl msg = null;
        ContentHeaderProperties prop = messageWrap.getContentHeaderProperties();
        byte[] payload = getPayload(prop.getBodySize().intValue(), messageWrap.getBodyParts());
        if (prop.getContentType() != null && prop.getContentType().startsWith("text/")) {
            TextMessageImpl textMessage = new TextMessageImpl();
            try {
                textMessage.setText(new String(payload, "utf-8"));
            } catch (UnsupportedEncodingException e) {
                throw new JMSException(e.toString());
            }
            msg = textMessage;
        } else
            msg = new BytesMessageImpl(payload, payload.length);
        msg.setLongProperty(messageFormat, Util.MESSAGE_FORMAT);
        transformHeader(msg, prop, destinationFactory);
        return msg;
    }
}
