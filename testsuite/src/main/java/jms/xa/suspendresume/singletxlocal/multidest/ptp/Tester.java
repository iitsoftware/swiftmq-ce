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

package jms.xa.suspendresume.singletxlocal.multidest.ptp;

import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends SimpleConnectedXAPTPTestCase {
    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(3);
    }

    public void testNP() {
        try {
            Xid xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            TextMessage msg = qs.createTextMessage();
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 2; i++) {
                    msg.setText("Msg1: " + i);
                    addSender[j].send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                }
            }
            xares.end(xid, XAResource.TMSUSPEND);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 3; i++) {
                    msg.setText("Msg2: " + i);
                    addSender[j].send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                }
            }
            qs.getQueueSession().commit();
            xares.start(xid, XAResource.TMRESUME);
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);

            xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 3; i++) {
                    msg = (TextMessage) addReceiver[j].receive(2000);
                    assertTrue("Received msg==null", msg != null);
                }
            }
            xares.end(xid, XAResource.TMSUSPEND);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 2; i++) {
                    msg = (TextMessage) addReceiver[j].receive(2000);
                    assertTrue("Received msg==null", msg != null);
                }
            }
            qs.getQueueSession().commit();
            xares.start(xid, XAResource.TMRESUME);
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);
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
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 2; i++) {
                    msg.setText("Msg1: " + i);
                    addSender[j].send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                }
            }
            xares.end(xid, XAResource.TMSUSPEND);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 3; i++) {
                    msg.setText("Msg2: " + i);
                    addSender[j].send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                }
            }
            qs.getQueueSession().commit();
            xares.start(xid, XAResource.TMRESUME);
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);

            xid = new XidImpl();
            xares.start(xid, XAResource.TMNOFLAGS);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 3; i++) {
                    msg = (TextMessage) addReceiver[j].receive(2000);
                    assertTrue("Received msg==null", msg != null);
                }
            }
            xares.end(xid, XAResource.TMSUSPEND);
            for (int j = 0; j < 3; j++) {
                for (int i = 0; i < 2; i++) {
                    msg = (TextMessage) addReceiver[j].receive(2000);
                    assertTrue("Received msg==null", msg != null);
                }
            }
            qs.getQueueSession().commit();
            xares.start(xid, XAResource.TMRESUME);
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.commit(xid, false);
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }
}
