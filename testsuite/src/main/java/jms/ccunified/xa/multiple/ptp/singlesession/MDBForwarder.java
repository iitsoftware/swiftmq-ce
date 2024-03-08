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

package jms.ccunified.xa.multiple.ptp.singlesession;

import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.UnifiedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class MDBForwarder extends UnifiedXAPTPTestCase {
    InitialContext ctx = null;
    XAConnection qc = null;
    Queue queueListen = null;
    Queue queueForward = null;
    ConnectionConsumer cc = null;
    ServerSessionPoolImpl pool = null;
    int nMsgs = 0;
    String listenQueue = null;
    String forwardQueue = null;

    public MDBForwarder(String name, String listenQueue, String forwardQueue) {
        super(name);
        this.listenQueue = listenQueue;
        this.forwardQueue = forwardQueue;
    }

    protected void setUp() throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createXAConnection(qcfName, false);
        queueListen = getQueue(listenQueue);
        queueForward = getQueue(forwardQueue);
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 5; i++) {
            XASession qs = qc.createXASession();
            qs.setMessageListener(new Listener(qs, i));
            pool.addServerSession(new ServerSessionImpl(pool, qs));
        }
        cc = qc.createConnectionConsumer(queueListen, null, pool, 5);
    }

    synchronized void inc() {
        nMsgs++;
        if (nMsgs == 1000)
            notify();
    }

    public void test() {
        try {
            synchronized (this) {
                qc.start();
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
        } catch (Exception e) {
            failFast("Test failed: " + e.toString());
        }
    }

    protected void tearDown() throws Exception {
        qc.close();
        super.tearDown();
    }

    private class Listener implements MessageListener {
        XASession session;
        XASession qs2;
        int id;

        public Listener(XASession session, int id) {
            this.session = session;
            this.id = id;
            try {
                qs2 = qc.createXASession();
            } catch (Exception e) {
                throw new RuntimeException(e.toString());
            }
        }

        public void onMessage(Message msg) {
            try {
                XAResource xares1 = session.getXAResource();  // done during JTA enlistment
                Xid xid = new XidImpl();                      // done during JTA enlistment
                xares1.start(xid, XAResource.TMNOFLAGS);       // done during JTA enlistment

                // Start: MDB's onMessage
                XAResource xares2 = qs2.getXAResource();       // done during JTA enlistment
                xares2.start(xid, XAResource.TMNOFLAGS);       // done during JTA enlistment
                System.out.println(listenQueue + " received: " + ((TextMessage) msg).getText() + " forward to: " + forwardQueue);
                MessageProducer producer = qs2.getSession().createProducer(queueForward);
                producer.send(msg);
                // End: MDB's onMessage

                xares1.end(xid, XAResource.TMSUCCESS);        // done by the JTA
                xares1.prepare(xid);                         // done by the JTA
                xares2.end(xid, XAResource.TMSUCCESS);        // done by the JTA
                xares2.prepare(xid);                         // done by the JTA
                xares1.commit(xid, false);                    // done by the JTA
                xares2.commit(xid, false);                    // done by the JTA

                producer.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            inc();
        }
    }
}
