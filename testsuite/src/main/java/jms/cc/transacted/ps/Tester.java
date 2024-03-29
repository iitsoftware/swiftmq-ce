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

package jms.cc.transacted.ps;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.PSTestCase;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;

import javax.jms.*;
import javax.naming.InitialContext;

public class Tester extends PSTestCase {
    InitialContext ctx = null;
    TopicConnection tc = null;
    TopicConnection tc1 = null;
    TopicSession qs = null;
    TopicPublisher publisher = null;
    Topic topic = null;
    ConnectionConsumer cc = null;
    ServerSessionPoolImpl pool = null;
    int nMsgs = 0;
    Semaphore sem = new Semaphore();

    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        String tcfName = System.getProperty("jndi.tcf");
        assertNotNull("missing property 'jndi.tcf'", tcfName);
        tc = createTopicConnection(tcfName, false);
        String topicName = System.getProperty("jndi.topic");
        assertNotNull("missing property 'jndi.topic'", topicName);
        topic = getTopic(topicName);
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 10; i++) {
            TopicSession qs = tc.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
            qs.setMessageListener(new Listener(qs, i));
            pool.addServerSession(new ServerSessionImpl(pool, qs));
        }
        cc = tc.createConnectionConsumer(topic, null, pool, 5);
        tc1 = createTopicConnection(tcfName, true);
        qs = tc1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        publisher = qs.createPublisher(topic);
    }

    synchronized void inc() {
        nMsgs++;
        if (nMsgs == 10000)
            sem.notifySingleWaiter();
    }

    public void test() {
        try {
            tc.start();
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10000; i++) {
                msg.setText("Msg: " + (i + 1));
                publisher.publish(msg);
            }

            sem.waitHere();
        } catch (Exception e) {
            failFast("Test failed: " + e.toString());
        }
    }

    protected void tearDown() throws Exception {
        sem.reset();
        sem.waitHere(1000);
        tc.close();
        tc1.close();
        super.tearDown();
    }

    private class Listener implements MessageListener {
        TopicSession session;
        int id;

        public Listener(TopicSession session, int id) {
            this.session = session;
            this.id = id;
        }

        public void onMessage(Message msg) {
            try {
                session.commit();
            } catch (JMSException e) {
                e.printStackTrace();
            }
            inc();
        }
    }
}