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

package jms.ha.persistent.ptp.nontransacted.requestreply.autoack;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Requestor extends SimpleConnectedPTPTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;
    String requestQueueName = null;
    String replyQueueName = null;
    QueueSender requestSender = null;
    QueueReceiver replyReceiver = null;
    Queue replyQueue = null;

    public Requestor(String name, String requestQueueName, String replyQueueName) {
        super(name);
        this.requestQueueName = requestQueueName;
        this.replyQueueName = replyQueueName;
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE, false, false);
        requestSender = qs.createSender(getQueue(requestQueueName));
        replyQueue = getQueue(replyQueueName);
        replyReceiver = qs.createReceiver(getQueue(replyQueueName));
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void send() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < nMsgs; i++) {
                msg.setIntProperty("no", i);
                msg.setText("Msg: " + i);
                msg.setJMSReplyTo(replyQueue);
                requestSender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                verifier.add(replyReceiver.receive());
            }
            verifier.verify();

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        requestQueueName = null;
        replyQueueName = null;
        requestSender = null;
        replyReceiver = null;
        replyQueue = null;
        super.tearDown();
    }
}

