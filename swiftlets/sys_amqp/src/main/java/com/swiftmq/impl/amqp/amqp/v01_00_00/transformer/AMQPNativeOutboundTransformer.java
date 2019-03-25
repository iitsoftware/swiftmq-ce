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
import com.swiftmq.impl.amqp.amqp.v01_00_00.Delivery;
import com.swiftmq.jms.BytesMessageImpl;

import javax.jms.JMSException;

public class AMQPNativeOutboundTransformer extends OutboundTransformer {
    public void transform(Delivery delivery) throws AMQPException, JMSException {
        BytesMessageImpl msg = null;
        try {
            msg = (BytesMessageImpl) delivery.getMessage();
        } catch (ClassCastException e) {
            throw new JMSException("Unable to transform. JMS message is not a BytesMessage");
        }
        if (!msg.propertyExists(messageFormat))
            throw new JMSException("Message format property not set");

        if (!msg.propertyExists(amqpNative) || !msg.getBooleanProperty(amqpNative))
            throw new JMSException("Message is not an AMQP native transformation");

        byte[] body = msg._getBody();
        if (body == null) {
            msg.reset();
            body = new byte[(int) msg.getBodyLength()];
            msg.readBytes(body);
        }
        delivery.setData(body);
        delivery.setMessageFormat(msg.getLongProperty(messageFormat));
    }
}
