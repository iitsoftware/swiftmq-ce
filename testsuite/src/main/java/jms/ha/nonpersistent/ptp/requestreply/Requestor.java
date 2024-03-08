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

package jms.ha.nonpersistent.ptp.requestreply;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Requestor extends SimpleConnectedPTPTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    QueueReceiver tempReceiver = null;
    TemporaryQueue tempQueue = null;
    MsgNoVerifier verifier = null;

    public Requestor(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        receiver.close();
        tempQueue = qs.createTemporaryQueue();
        tempReceiver = qs.createReceiver(tempQueue);
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void request() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < nMsgs; i++) {
                TextMessage reply = null;
                do {
                    msg.setIntProperty("no", i);
                    msg.setJMSReplyTo(tempQueue);
                    msg.setText("Request: " + i);
                    sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                    reply = (TextMessage) tempReceiver.receive(60000);
                } while (reply == null);
            }
            msg.setIntProperty("no", nMsgs + 1);
            msg.setBooleanProperty("finished", true);
            sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        tempReceiver = null;
        tempQueue = null;
        verifier = null;
        super.tearDown();
    }
}

