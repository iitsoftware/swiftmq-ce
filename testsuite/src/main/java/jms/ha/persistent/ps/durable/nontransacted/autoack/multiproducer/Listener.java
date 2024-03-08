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

package jms.ha.persistent.ps.durable.nontransacted.autoack.multiproducer;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class Listener extends SimpleConnectedPSTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    int n = 0;
    Exception exception = null;
    TopicConnection tc1 = null;
    TopicConnection tc2 = null;
    TopicSession session1 = null;
    TopicSession session2 = null;
    TopicSubscriber subscriber1 = null;
    TopicSubscriber subscriber2 = null;

    public Listener(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE, true, false, true);
        tc1 = tcf.createTopicConnection();
        tc1.setClientID(nextCID());
        tc1.start();
        tc2 = tcf.createTopicConnection();
        tc2.setClientID(nextCID());
        tc2.start();
        session1 = tc1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        session2 = tc2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber1 = session1.createDurableSubscriber(getTopic("testtopic"), "dur1");
        subscriber2 = session2.createDurableSubscriber(getTopic("testtopic"), "dur2");
    }

    protected void tearDown() throws Exception {
        subscriber1.close();
        subscriber2.close();
        session1.unsubscribe("dur1");
        session2.unsubscribe("dur2");
        session1.close();
        session2.close();
        tc1.close();
        tc2.close();
        tc1 = null;
        tc2 = null;
        exception = null;
        session1 = null;
        session2 = null;
        subscriber1 = null;
        subscriber2 = null;
        super.tearDown();
    }

    public void receive() {
        try {
            Semaphore sem1 = new Semaphore();
            subscriber.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem1));
            Semaphore sem2 = new Semaphore();
            subscriber1.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem2));
            Semaphore sem3 = new Semaphore();
            subscriber2.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem3));
            sem1.waitHere();
            sem2.waitHere();
            sem3.waitHere();
            if (exception != null)
                throw exception;
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    private class MyListener implements MessageListener {
        MsgNoVerifier verifier = null;
        Semaphore sem = null;
        int n = 0;

        public MyListener(MsgNoVerifier verifier, Semaphore sem) {
            this.verifier = verifier;
            this.sem = sem;
            this.verifier.setCheckSequence(false);
            this.verifier.setDoublesOk(true);
        }

        public void onMessage(Message message) {
            try {
                verifier.add(message);
                n++;
                if (n == nMsgs)
                    sem.notifySingleWaiter();
            } catch (Exception e) {
                exception = e;
                sem.notifySingleWaiter();
            }
        }
    }
}