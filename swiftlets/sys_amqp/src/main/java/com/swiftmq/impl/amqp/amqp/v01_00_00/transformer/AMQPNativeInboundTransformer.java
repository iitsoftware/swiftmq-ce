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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.amqp.v100.client.AMQPException;
import com.swiftmq.amqp.v100.generated.transport.performatives.TransferFrame;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;

import javax.jms.JMSException;
import java.util.List;
import java.util.Map;

public class AMQPNativeInboundTransformer extends InboundTransformer {
    String messageFormat;

    private void setDefaultHeader(MessageImpl msg) throws JMSException {
        msg.setJMSMessageID(nextMsgId());
        msg.setJMSTimestamp(System.currentTimeMillis());
        if (defaultTtl > 0)
            msg.setJMSExpiration(System.currentTimeMillis() + defaultTtl);
        msg.setJMSDeliveryMode(defaultDeliveryMode);
        msg.setJMSPriority(defaultPriority);
    }

    public void setConfiguration(Map config) throws Exception {
        super.setConfiguration(config);
        messageFormat = prefixVendor + Util.PROP_MESSAGE_FORMAT;
    }

    public MessageImpl transform(TransferFrame frame, DestinationFactory destinationFactory) throws AMQPException, JMSException {
        BytesMessageImpl msg = null;
        if (frame.getMorePayloads() == null) {
            byte[] payload = frame.getPayload();
            msg = new BytesMessageImpl(payload, payload.length);
        } else {
            byte[] b = new byte[frame.getPayloadLength()];
            byte[] payload = frame.getPayload();
            System.arraycopy(payload, 0, b, 0, payload.length);
            int pos = payload.length;
            List morePayloads = frame.getMorePayloads();
            for (int i = 0; i < morePayloads.size(); i++) {
                payload = (byte[]) morePayloads.get(i);
                System.arraycopy(payload, 0, b, pos, payload.length);
                pos += payload.length;
            }
            msg = new BytesMessageImpl(b, b.length);
        }
        try {
            msg.setLongProperty(messageFormat, frame.getMessageFormat().getValue());
            msg.setBooleanProperty(amqpNative, true);
            setDefaultHeader(msg);
        } catch (Exception e) {
            throw new AMQPException(e.toString());
        }
        msg.reset();
        return msg;
    }
}
