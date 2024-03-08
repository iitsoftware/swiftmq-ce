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

package jms.func.prio;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.DeliveryMode;
import javax.jms.TextMessage;

public class Priority extends SimpleConnectedPTPTestCase {
    public Priority(String name) {
        super(name);
    }

    public void test_9_to_0() {
        try {
            receiver.close(); // to ensure prio order in the queue
            TextMessage msg = qs.createTextMessage();
            for (int i = 9; i >= 0; i--) {
                msg.setIntProperty("id", i);
                msg.setText("Prio: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, i, 0);
            }
            receiver = qs.createReceiver(queue);
            for (int i = 9; i >= 0; i--) {
                msg = (TextMessage) receiver.receive();
                int id = msg.getIntProperty("id");
                assertTrue("Does not receive right msg, expected: " + i + ", received: " + id, id == i);
            }
        } catch (Exception e) {
            failFast("test_9_to_0 failed: " + e);
        }
    }

    public void test_0_to_9() {
        try {
            receiver.close(); // to ensure prio order in the queue
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setIntProperty("id", i);
                msg.setText("Prio: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, i, 0);
            }

            receiver = qs.createReceiver(queue);
            for (int i = 9; i >= 0; i--) {
                msg = (TextMessage) receiver.receive();
                int id = msg.getIntProperty("id");
                assertTrue("Does not receive right msg, expected: " + i + ", received: " + id, id == i);
            }
        } catch (Exception e) {
            failFast("test_0_to_9 failed: " + e);
        }
    }

}

