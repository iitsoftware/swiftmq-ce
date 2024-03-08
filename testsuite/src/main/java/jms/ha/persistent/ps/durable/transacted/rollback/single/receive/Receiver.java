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

package jms.ha.persistent.ps.durable.transacted.rollback.single.receive;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPSTestCase;

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

public class Receiver extends SimpleConnectedPSTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;
    TopicSession s1 = null;
    TopicSession s2 = null;
    TopicSession s3 = null;
    TopicSubscriber subscriber1 = null;
    TopicSubscriber subscriber2 = null;
    TopicSubscriber subscriber3 = null;

    public Receiver(String name) {
        super(name);
    }

    protected void beforeCreateSession() throws Exception {
        s1 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
        s2 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
        s3 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
    }

    protected void afterCreateSession() throws Exception {
        s1.close();
        s2.close();
        s3.close();
    }

    protected void beforeCreateReceiver() throws Exception {
        subscriber1 = ts.createSubscriber(topic);
        subscriber2 = ts.createSubscriber(topic);
        subscriber3 = ts.createSubscriber(topic);
    }

    protected void afterCreateReceiver() throws Exception {
        subscriber1.close();
        subscriber2.close();
        subscriber3.close();
    }

    protected void setUp() throws Exception {
        setUp(true, Session.AUTO_ACKNOWLEDGE, true, false, true);
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void receive() {
        try {
            boolean rollback = false;
            int n = 0, m = 0;
            while (n < nMsgs) {
                Message msg = subscriber.receive();
                if (msg == null)
                    throw new Exception("null message received!");
                if (rollback) {
                    System.out.println("during rollback, ignore: " + msg.getIntProperty("no"));
                    m++;
                    if (m == 10) {
                        System.out.println("rollback session!");
                        ts.rollback();
                        rollback = false;
                        m = 0;
                    }
                } else {
                    System.out.println("accepted: " + msg.getIntProperty("no"));
                    verifier.add(msg);
                    n++;
                    if (n % 10 == 0) {
                        System.out.println("commit session!");
                        ts.commit();
                        rollback = true;
                    }
                }
            }
            verifier.verify();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        s1 = null;
        s2 = null;
        s3 = null;
        subscriber1 = null;
        subscriber2 = null;
        subscriber3 = null;
        super.tearDown();
    }
}

