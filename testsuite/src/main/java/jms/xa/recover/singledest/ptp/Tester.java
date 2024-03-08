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

package jms.xa.recover.singledest.ptp;

import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends SimpleConnectedXAPTPTestCase {
    public Tester(String name) {
        super(name);
    }

    public void testNP() {
        try {
            Xid xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 5; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    public void testP() {
        try {
            Xid xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 5; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    public void testRecoverRollback() {
        try {
            Xid[] xids = xares.recover(XAResource.TMNOFLAGS);
            assertTrue("Invalid number of xids. Expected 2, taken: " + xids.length, xids.length == 2);
            for (int i = 0; i < xids.length; i++) {
                xares.rollback(xids[i]);
            }
            xids = xares.recover(XAResource.TMNOFLAGS);
            assertTrue("Invalid number of xids. Expected 0, taken: " + xids.length, xids.length == 0);
        } catch (XAException e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    public void testRecoverCommit() {
        try {
            Xid[] xids = xares.recover(XAResource.TMNOFLAGS);
            assertTrue("Invalid number of xids. Expected 2, taken: " + xids.length, xids.length == 2);
            for (int i = 0; i < xids.length; i++) {
                xares.commit(xids[i], false);
            }
            xids = xares.recover(XAResource.TMNOFLAGS);
            assertTrue("Invalid number of xids. Expected 0, taken: " + xids.length, xids.length == 0);

            Xid xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = null;
            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }
}
