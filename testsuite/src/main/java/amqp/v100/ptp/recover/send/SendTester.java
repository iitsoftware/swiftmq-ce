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

package amqp.v100.ptp.recover.send;

import amqp.v100.base.MessageFactory;
import amqp.v100.base.ReceiveVerifier;
import amqp.v100.base.Util;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.ApplicationProperties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPInt;
import com.swiftmq.amqp.v100.types.AMQPString;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class SendTester extends TestCase {
    private static String PROPNAME = "no";
    int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "1000"));
    boolean persistent = Boolean.parseBoolean(System.getProperty("persistent", "true"));

    MessageFactory messageFactory;
    int qos;
    String address = null;
    ReceiveVerifier receiveVerifier = null;

    public SendTester(String name, int qos, String address, boolean dupsOk, boolean missesOk) {
        super(name);
        this.qos = qos;
        this.address = address;
        receiveVerifier = new ReceiveVerifier(this, nMsgs, PROPNAME, dupsOk, missesOk);
    }

    protected void setUp() throws Exception {
        super.setUp();
        messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
    }

    public void test() {
        try {
            Connection connection = Util.createConnection();
            Session session = Util.createSession(connection);
            Producer producer = session.createProducer(address, qos);
            for (int i = 0; i < nMsgs; i++) {
                AMQPMessage msg = messageFactory.create(i);
                Map map = new HashMap();
                map.put(new AMQPString(PROPNAME), new AMQPInt(i));
                msg.setApplicationProperties(new ApplicationProperties(map));
                producer.send(msg, persistent, 5, -1);
            }
            connection.close();
            DeliveryMemory deliveryMemory = producer.getDeliveryMemory();
            System.out.println("Unsettled: " + deliveryMemory.getNumberUnsettled());
            connection = Util.createConnection();
            session = Util.createSession(connection);
            Producer producerRecover = session.createProducer(address, qos, deliveryMemory);
            producerRecover.close();
            session.close();
            connection.close();

            connection = Util.createConnection();
            session = Util.createSession(connection);
            Consumer consumer = session.createConsumer(address, 500, qos, true, null);
            for (; ; ) {
                AMQPMessage msg = consumer.receive(1000);
                if (msg != null) {
                    messageFactory.verify(msg);
                    if (!msg.isSettled())
                        msg.accept();
                    receiveVerifier.add(msg);
                } else
                    break;
            }
            consumer.close();
            session.close();
            connection.close();
            receiveVerifier.verify();
        } catch (Exception e) {
            e.printStackTrace();
            fail("test failed: " + e);
        }
    }
}
