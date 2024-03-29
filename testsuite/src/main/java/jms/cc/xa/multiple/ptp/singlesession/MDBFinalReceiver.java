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

package jms.cc.xa.multiple.ptp.singlesession;

import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.XAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class MDBFinalReceiver extends XAPTPTestCase {
    InitialContext ctx = null;
    XAQueueConnection qc = null;
    Queue queueListen = null;
    ConnectionConsumer cc = null;
    ServerSessionPoolImpl pool = null;
    int nMsgs = 0;
    String listenQueue = null;

    public MDBFinalReceiver(String name, String listenQueue) {
        super(name);
        this.listenQueue = listenQueue;
    }

    protected void setUp() throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createXAQueueConnection(qcfName, false);
        queueListen = getQueue(listenQueue);
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 5; i++) {
            XAQueueSession qs = qc.createXAQueueSession();
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
        XAQueueSession session;
        int id;

        public Listener(XAQueueSession session, int id) {
            this.session = session;
            this.id = id;
        }

        public void onMessage(Message msg) {
            try {
                XAResource xares1 = session.getXAResource();  // done during JTA enlistment
                Xid xid = new XidImpl();                      // done during JTA enlistment
                xares1.start(xid, XAResource.TMNOFLAGS);       // done during JTA enlistment

                // Start: MDB's onMessage
                System.out.println(listenQueue + " received: " + ((TextMessage) msg).getText());
                // End: MDB's onMessage

                xares1.end(xid, XAResource.TMSUCCESS);        // done by the JTA
                xares1.prepare(xid);                         // done by the JTA
                xares1.commit(xid, false);                    // done by the JTA
            } catch (Exception e) {
                e.printStackTrace();
            }
            inc();
        }
    }
}
