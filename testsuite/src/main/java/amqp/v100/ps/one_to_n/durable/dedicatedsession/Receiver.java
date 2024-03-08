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

package amqp.v100.ps.one_to_n.durable.dedicatedsession;

import amqp.v100.base.AMQPConnectedSessionTestCase;
import amqp.v100.base.MessageFactory;
import com.swiftmq.amqp.v100.client.DurableConsumer;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;

public class Receiver extends AMQPConnectedSessionTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("nmsgs", "100000"));
    int linkCredit = Integer.parseInt(System.getProperty("linkcredit", "500"));

    MessageFactory messageFactory;
    int qos;
    String address = null;
    DurableConsumer consumer = null;

    public Receiver(String name, int qos, String address, String containerId) {
        super(name);
        this.qos = qos;
        this.address = address;
        setContainerId(containerId);
    }

    protected void setUp() throws Exception {
        super.setUp();
        consumer = getSession().createDurableConsumer("testdurable", address, linkCredit, qos, true, null);
        messageFactory = (MessageFactory) Class.forName(System.getProperty("messagefactory", "amqp.v100.base.AMQPValueStringMessageFactory")).newInstance();
    }

    public void receive() {
        try {
            for (int i = 0; i < nMsgs; i++) {
                AMQPMessage msg = consumer.receive();
                if (msg != null) {
                    messageFactory.verify(msg);
                    if (!msg.isSettled())
                        msg.accept();
                } else
                    throw new Exception("Msg == null at i=" + i);
            }
        } catch (Exception e) {
            fail("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
            consumer.unsubscribe();
        }
        super.tearDown();
    }

}
