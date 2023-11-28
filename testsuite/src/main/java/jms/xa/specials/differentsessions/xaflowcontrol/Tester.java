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

package jms.xa.specials.differentsessions.xaflowcontrol;

import com.swiftmq.tools.concurrent.Semaphore;
import jms.base.MultisessionConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends MultisessionConnectedXAPTPTestCase {
    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp(2);
    }

    public void testP() {
        try {
            Semaphore sem = new Semaphore();
            new Receiver(sem).start();
            XAResource xares1 = sessions[0].getXAResource();
            QueueSender sender1 = sessions[0].getQueueSession().createSender(queue);
            TextMessage msg = sessions[0].createTextMessage();
            for (int i = 0; i < 10000; i++) {
                Xid xid = new XidImpl();
                xares1.start(xid, XAResource.TMNOFLAGS);
                msg.setText("Msg1: " + i);
                sender1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                xares1.end(xid, XAResource.TMSUCCESS);
                xares1.prepare(xid);
                xares1.commit(xid, false);
            }
            sem.waitHere();
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    private class Receiver extends Thread {
        Semaphore sem = null;

        public Receiver(Semaphore sem) {
            this.sem = sem;
        }

        public void run() {
            try {
                XAResource xares2 = sessions[1].getXAResource();
                QueueReceiver receiver2 = sessions[1].getQueueSession().createReceiver(queue);
                TextMessage msg = null;
                for (int i = 0; i < 10000; i++) {
                    XidImpl xid = new XidImpl();
                    xares2.start(xid, XAResource.TMNOFLAGS);
                    msg = (TextMessage) receiver2.receive(2000);
                    assertTrue("Received msg==null", msg != null);
                    pause(10);
                    xares2.end(xid, XAResource.TMSUCCESS);
                    xares2.prepare(xid);
                    xares2.commit(xid, false);
                }
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use Options | File Templates.
            }
            sem.notifySingleWaiter();
        }
    }
}
