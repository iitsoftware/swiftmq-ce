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

package jms.xa.specials.differentsessions.tmjoin;

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
        super.setUp(4);
    }

    public void testP() {
        try {
            Xid xid = new XidImpl();
            XAResource xares1 = sessions[0].getXAResource();
            QueueSender sender1 = sessions[0].getQueueSession().createSender(queue);
            xares1.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = sessions[0].createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg1: " + i);
                sender1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            xares1.end(xid, XAResource.TMSUCCESS);
            xares1.prepare(xid);
            xares1.commit(xid, false);

            xid = new XidImpl();
            QueueReceiver receiver1 = sessions[0].getQueueSession().createReceiver(queue);
            xares1.start(xid, XAResource.TMNOFLAGS);
            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg = (TextMessage) receiver1.receive(2000);
            assertTrue("Received msg!=null", msg == null);

            XAResource xares2 = sessions[1].getXAResource();
            QueueSender sender2 = sessions[1].getQueueSession().createSender(queue);
            xares2.start(xid, XAResource.TMJOIN);
            msg = sessions[1].createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg2: " + i);
                sender2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            XAResource xares3 = sessions[2].getXAResource();
            QueueSender sender3 = sessions[2].getQueueSession().createSender(queue);
            xares3.start(xid, XAResource.TMJOIN);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg2: " + i);
                sender3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            XAResource xares4 = sessions[3].getXAResource();
            QueueSender sender4 = sessions[3].getQueueSession().createSender(queue);
            xares4.start(xid, XAResource.TMJOIN);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg2: " + i);
                sender4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            xares1.end(xid, XAResource.TMSUCCESS);
            xares2.end(xid, XAResource.TMSUCCESS);
            xares3.end(xid, XAResource.TMSUCCESS);
            xares4.end(xid, XAResource.TMSUCCESS);

            xares2.prepare(xid);
            xares3.commit(xid, false);

            xid = new XidImpl();
            xares1.start(xid, XAResource.TMNOFLAGS);
            for (int i = 0; i < 30; i++) {
                msg = (TextMessage) receiver1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg = (TextMessage) receiver1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            xares1.end(xid, XAResource.TMSUCCESS);
            xares1.prepare(xid);
            xares4.commit(xid, false);
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }
}
