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
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpSequence;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPMap;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.jms.*;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import java.io.IOException;
import java.util.*;

public class MessageTypeBodyFactory implements BodyFactory {
    public void createBody(MessageImpl jmsMessage, AMQPMessage amqpMessage) throws JMSException, AMQPException {
        jmsMessage.reset();
        if (jmsMessage instanceof TextMessageImpl) {
            amqpMessage.setAmqpValue(new AmqpValue(new AMQPString(((TextMessageImpl) jmsMessage).getText())));
        } else if (jmsMessage instanceof MapMessageImpl) {
            MapMessageImpl msg = (MapMessageImpl) jmsMessage;
            Map map = new HashMap();
            for (Enumeration _enum = msg.getMapNames(); _enum.hasMoreElements(); ) {
                String name = (String) _enum.nextElement();
                map.put(new AMQPString(name), Util.convertJMStoAMQP(msg.getObject(name)));
            }
            try {
                amqpMessage.setAmqpValue(new AmqpValue(new AMQPMap(map)));
            } catch (IOException e) {
                throw new AMQPException(e.toString());
            }
        } else if (jmsMessage instanceof BytesMessageImpl) {
            BytesMessageImpl msg = (BytesMessageImpl) jmsMessage;
            byte[] b = new byte[(int) msg.getBodyLength()];
            msg.readBytes(b);
            amqpMessage.addData(new Data(b));
        } else if (jmsMessage instanceof ObjectMessageImpl) {
            amqpMessage.setAmqpValue(new AmqpValue(Util.convertJMStoAMQP(((ObjectMessageImpl) jmsMessage).getObject())));
        } else if (jmsMessage instanceof StreamMessageImpl) {
            List list = new ArrayList();
            StreamMessageImpl msg = (StreamMessageImpl) jmsMessage;
            try {
                for (; ; ) {
                    list.add(Util.convertJMStoAMQP(msg.readObject()));
                }
            } catch (MessageEOFException e) {
            }
            try {
                amqpMessage.addAmqpSequence(new AmqpSequence(list));
            } catch (IOException e) {
                throw new AMQPException(e.toString());
            }
        }
    }
}
