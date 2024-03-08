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

package jms.func.nontransacted.clientack;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class PTPMultipleQueues extends SimpleConnectedPTPTestCase {
    Queue m1 = null;
    Queue m2 = null;
    Queue m3 = null;
    Queue m4 = null;
    Queue m5 = null;
    QueueSender uisender = null;
    QueueSender qsm1 = null;
    QueueSender qsm2 = null;
    QueueSender qsm3 = null;
    QueueSender qsm4 = null;
    QueueSender qsm5 = null;
    QueueReceiver qrm1 = null;
    QueueReceiver qrm2 = null;
    QueueReceiver qrm3 = null;
    QueueReceiver qrm4 = null;
    QueueReceiver qrm5 = null;

    public PTPMultipleQueues(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.CLIENT_ACKNOWLEDGE);
        createQueue("m1");
        createQueue("m2");
        createQueue("m3");
        createQueue("m4");
        createQueue("m5");
        m1 = (Queue) ctx.lookup("m1@router");
        m2 = (Queue) ctx.lookup("m2@router");
        m3 = (Queue) ctx.lookup("m3@router");
        m4 = (Queue) ctx.lookup("m4@router");
        m5 = (Queue) ctx.lookup("m5@router");
        uisender = qs.createSender(null);
        qsm1 = qs.createSender(m1);
        qsm2 = qs.createSender(m2);
        qsm3 = qs.createSender(m3);
        qsm4 = qs.createSender(m4);
        qsm5 = qs.createSender(m5);
        qrm1 = qs.createReceiver(m1);
        qrm2 = qs.createReceiver(m2);
        qrm3 = qs.createReceiver(m3);
        qrm4 = qs.createReceiver(m4);
        qrm5 = qs.createReceiver(m5);
    }

    public void testPTPUnidentifiedNP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uisender.send(m1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPIdentifiedNP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                qsm1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPUnidentifiedP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uisender.send(m1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uisender.send(m5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPIdentifiedP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                qsm1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();

            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPSendReceiveNP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                qsm1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();


            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPTPSendReceiveP() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                qsm1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                qsm5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qrm5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            msg.acknowledge();


            msg = (TextMessage) qrm1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) qrm5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        uisender.close();
        qsm1.close();
        qsm2.close();
        qsm3.close();
        qsm4.close();
        qsm5.close();
        qrm1.close();
        qrm2.close();
        qrm3.close();
        qrm4.close();
        qrm5.close();
        deleteQueue("m1");
        deleteQueue("m2");
        deleteQueue("m3");
        deleteQueue("m4");
        deleteQueue("m5");
        super.tearDown();
    }
}

