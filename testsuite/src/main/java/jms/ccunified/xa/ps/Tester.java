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

package jms.ccunified.xa.ps;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.UnifiedXAPSTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends UnifiedXAPSTestCase {
    InitialContext ctx = null;
    XAConnection tc = null;
    XAConnection tc1 = null;
    XASession ts = null;
    MessageProducer producer = null;
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
        tc = createXAConnection(tcfName, null, false);
        String topicName = System.getProperty("jndi.topic");
        assertNotNull("missing property 'jndi.topic'", topicName);
        topic = getTopic(topicName);
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 50; i++) {
            XASession ts = tc.createXASession();
            ts.setMessageListener(new Listener(ts, i));
            pool.addServerSession(new ServerSessionImpl(pool, ts));
        }
        cc = tc.createConnectionConsumer(topic, null, pool, 5);
        tc1 = createXAConnection(tcfName, null, true);
        ts = tc1.createXASession();
        producer = ts.getSession().createProducer(topic);
    }

    synchronized void inc() {
        nMsgs++;
        if (nMsgs == 10000)
            sem.notifySingleWaiter();
    }

    public void test() {
        try {
            tc.start();
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10000; i++) {
                msg.setText("Msg: " + (i + 1));
                producer.send(msg);
                ts.getSession().commit();
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
        XASession session;
        int id;

        public Listener(XASession session, int id) {
            this.session = session;
            this.id = id;
        }

        public void onMessage(Message msg) {
            try {
                XAResource xares = session.getXAResource();
                Xid xid = new XidImpl();
                xares.start(xid, XAResource.TMNOFLAGS);
                xares.end(xid, XAResource.TMSUCCESS);
                xares.prepare(xid);
                xares.commit(xid, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
            inc();
        }
    }
}
