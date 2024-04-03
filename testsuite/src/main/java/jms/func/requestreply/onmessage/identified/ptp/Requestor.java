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

package jms.func.requestreply.onmessage.identified.ptp;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Requestor extends SimpleConnectedPTPTestCase {
    QueueReceiver tempReceiver = null;
    TemporaryQueue tempQueue = null;

    public Requestor(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        tempQueue = qs.createTemporaryQueue();
        tempReceiver = qs.createReceiver(tempQueue);
    }

    public void testRequest() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setJMSReplyTo(tempQueue);
            for (int i = 0; i < 1000; i++) {
                msg.setText("Request: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                TextMessage reply = (TextMessage) tempReceiver.receive();
            }
            pause(3000);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

