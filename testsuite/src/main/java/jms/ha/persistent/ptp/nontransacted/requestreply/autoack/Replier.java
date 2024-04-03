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

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Replier extends SimpleConnectedPTPTestCase implements MessageListener {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;
    int n = 0;
    Exception exception = null;
    Semaphore sem = null;

    public Replier(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE, false, true);
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void onMessage(Message message) {
        try {
            verifier.add(message);
            QueueSender replySender = qs.createSender((Queue) message.getJMSReplyTo());
            replySender.send(message, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            replySender.close();
            n++;
            if (n == nMsgs)
                sem.notifySingleWaiter();
        } catch (Exception e) {
            exception = e;
            sem.notifySingleWaiter();
        }
    }

    public void receive() {
        try {
            sem = new Semaphore();
            receiver.setMessageListener(this);
            sem.waitHere();
            if (exception != null)
                throw exception;
            verifier.verify();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        exception = null;
        sem = null;
        super.tearDown();
    }
}

