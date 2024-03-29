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

package jms.ha.nonpersistent.ps.nondurable.transacted;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Sender extends SimpleConnectedPSTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));

    public Sender(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(true, Session.AUTO_ACKNOWLEDGE, true, true, false);
        pause(60000);
    }

    public void send() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < nMsgs; i++) {
                msg.setIntProperty("no", i);
                msg.setText("Msg: " + i);
                publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                if ((i + 1) % 10 == 0)
                    ts.commit();
            }

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

