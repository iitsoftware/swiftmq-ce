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

package jms.ha.persistent.ps.durable.cc.singlesession.autoack;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.MsgNoVerifier;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class Consumer extends SimpleConnectedPSTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;
    ConnectionConsumer cc = null;
    ServerSessionPoolImpl pool = null;
    Semaphore sem = null;
    Exception exception = null;

    public Consumer(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp(false, Session.AUTO_ACKNOWLEDGE, true, false, true);
        subscriber.close();
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 1; i++) {
            TopicSession session = tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            session.setMessageListener(new Listener());
            pool.addServerSession(new ServerSessionImpl(pool, session));
        }
        cc = tc.createDurableConnectionConsumer(topic, "dur", null, pool, 5);
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void consume() {
        sem = new Semaphore();
        sem.waitHere();
        if (exception != null)
            failFast("failed: " + exception);
    }

    protected void tearDown() throws Exception {
        cc.close();
        verifier = null;
        cc = null;
        pool = null;
        sem = null;
        exception = null;
        super.tearDown();
    }

    private class Listener implements MessageListener {
        int n = 0;

        public void onMessage(Message msg) {
            n++;
            try {
                verifier.add(msg);
            } catch (Exception e) {
                exception = e;
                sem.notifySingleWaiter();
            }
            if (n == nMsgs) {
                try {
                    verifier.verify();
                } catch (Exception e) {
                    exception = e;
                }
                sem.notifySingleWaiter();
            }
        }
    }
}
