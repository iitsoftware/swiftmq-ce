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

package jms.ccunified.nontransacted.clientack.ptp;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.UnifiedPTPTestCase;

import javax.jms.*;
import javax.naming.InitialContext;

public class Tester extends UnifiedPTPTestCase {
    InitialContext ctx = null;
    Connection qc = null;
    Connection qc1 = null;
    Session qs = null;
    MessageProducer producer = null;
    MessageConsumer consumer = null;
    Queue queue = null;
    ConnectionConsumer cc = null;
    ServerSessionPoolImpl pool = null;
    int nMsgs = 0;
    Semaphore sem = new Semaphore();

    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createConnection(qcfName, false);
        String queueName = System.getProperty("jndi.queue");
        assertNotNull("missing property 'jndi.queue'", queueName);
        queue = getQueue(queueName);
        pool = new ServerSessionPoolImpl();
        for (int i = 0; i < 10; i++) {
            Session qs = qc.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            qs.setMessageListener(new Listener(i));
            pool.addServerSession(new ServerSessionImpl(pool, qs));
        }
        cc = qc.createConnectionConsumer(queue, null, pool, 5);
        qc1 = createConnection(qcfName, true);
        qs = qc1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = qs.createProducer(queue);
        consumer = qs.createConsumer(queue);
    }

    synchronized void inc() {
        nMsgs++;
        if (nMsgs == 10000)
            sem.notifySingleWaiter();
    }

    public void test() {
        try {
            qc.start();
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10000; i++) {
                msg.setText("Msg: " + (i + 1));
                producer.send(msg);
            }

            sem.waitHere();
            msg = (TextMessage) consumer.receive(2000);
            assertTrue("Msg != null", msg == null);
        } catch (Exception e) {
            failFast("Test failed: " + e.toString());
        }
    }

    protected void tearDown() throws Exception {
        sem.reset();
        sem.waitHere(1000);
        qc.close();
        qc1.close();
        super.tearDown();
    }

    private class Listener implements MessageListener {
        int id;

        public Listener(int id) {
            this.id = id;
        }

        public void onMessage(Message msg) {
            try {
                msg.acknowledge();
            } catch (JMSException e) {
                e.printStackTrace();
            }
            inc();
        }
    }
}
