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

package jms.xaunified.specials.redelivered;

import jms.base.SimpleConnectedUnifiedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends SimpleConnectedUnifiedXAPTPTestCase {
    public Tester(String name) {
        super(name);
    }

    public void testP() {
        try {
            Xid xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setIntProperty("id", i);
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);

            xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            int ids[] = new int[3];
            for (int i = 0; i < 3; i++) {
                msg = (TextMessage) consumer.receive();
                ids[i] = msg.getIntProperty("id");
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.rollback(xid);

            xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            for (int i = 0; i < 3; i++) {
                msg = (TextMessage) consumer.receive();
                int id = msg.getIntProperty("id");
                assertTrue("Does not receive right msg, expected: " + ids[i] + ", received: " + id, id == ids[i]);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);

            xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            for (int i = 3; i < 10; i++) {
                msg = (TextMessage) consumer.receive();
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg marked as redelivered", !redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 1);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

