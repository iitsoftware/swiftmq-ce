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

package jms.func.nontransacted.clientack;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

public class PTPSendSingleQueue extends SimpleConnectedPTPTestCase {
    public PTPSendSingleQueue(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.CLIENT_ACKNOWLEDGE);
    }

    public void testPTPSendSingleQueueNP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPSendSingleQueueP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

