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

package jms.ha.persistent.ptp.cluster.nontransacted.clientack.single.nonrecover.onmessage;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPClusterTestCase;

import javax.jms.*;

public class Listener extends SimpleConnectedPTPClusterTestCase implements MessageListener {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));
    long initDelay = Long.parseLong(System.getProperty("jms.ha.cluster.receive.initdelay", "20000"));
    MsgNoVerifier verifier = null;
    int n = 0;
    Exception exception = null;
    Semaphore sem = null;
    QueueSession s1 = null;
    QueueSession s2 = null;
    QueueSession s3 = null;
    QueueReceiver receiver1 = null;
    QueueReceiver receiver2 = null;
    QueueReceiver receiver3 = null;

    public Listener(String name) {
        super(name);
    }

    protected void beforeCreateSession() throws Exception {
        s1 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
        s2 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
        s3 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    protected void afterCreateSession() throws Exception {
        s1.close();
        s2.close();
        s3.close();
    }

    protected void beforeCreateReceiver() throws Exception {
        receiver1 = qs.createReceiver(queue);
        receiver2 = qs.createReceiver(queue);
        receiver3 = qs.createReceiver(queue);
    }

    protected void afterCreateReceiver() throws Exception {
        receiver1.close();
        receiver2.close();
        receiver3.close();
    }

    protected void setUp() throws Exception {
        pause(initDelay);
        setUp(false, Session.CLIENT_ACKNOWLEDGE, false, true);
        verifier = new MsgNoVerifier(this, nMsgs, "no", true);
        verifier.setCheckSequence(false);
    }

    public void onMessage(Message message) {
        try {
            verifier.add(message);
            n++;
            if (n % 10 == 0)
                message.acknowledge();
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
        s1 = null;
        s2 = null;
        s3 = null;
        receiver1 = null;
        receiver2 = null;
        receiver3 = null;
        super.tearDown();
    }

}

