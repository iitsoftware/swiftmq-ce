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

package jms.funcunified.transacted.multisubscriber;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Publisher extends SimpleConnectedUnifiedPSTestCase {
    Object sem = new Object();

    public Publisher(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(true, Session.CLIENT_ACKNOWLEDGE);
        pause(10000);
    }

    public void testPublishNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPublishP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

