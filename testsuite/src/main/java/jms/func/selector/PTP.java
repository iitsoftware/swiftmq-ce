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

package jms.func.selector;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class PTP extends SimpleConnectedPTPTestCase {
    QueueReceiver qr1 = null;
    QueueReceiver qr2 = null;
    QueueReceiver qr3 = null;
    QueueReceiver qr4 = null;
    QueueReceiver qr5 = null;
    QueueReceiver qr6 = null;
    QueueReceiver qr7 = null;
    QueueReceiver qr8 = null;

    public PTP(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        qr1 = qs.createReceiver(queue, "Int1 between 90 and 120 and time >= " + System.currentTimeMillis());
        qr2 = qs.createReceiver(queue, "D1 > 1.0 and D1 < 1000.0");
        qr3 = qs.createReceiver(queue, "Int2=200 and Int1=400 and S1 like 'Moin%'");
        qr4 = qs.createReceiver(queue, "D1 > 1.0 and D1 < 1000.0 and S1 = 'Moin Moin'");
        qr5 = qs.createReceiver(queue, "(HHLA_Aspect IS NOT NULL) AND ((HHLA_Aspect IN ('Aspect', 'oops')))");
        qr6 = qs.createReceiver(queue, "prop = 'Nation/World'");
        qr7 = qs.createReceiver(queue, "prop = 'Nation''s World'");
        qr8 = qs.createReceiver(queue, "vdmConnectionSource = 'mbenson-ncm2.\u0183\u0286\u0382?\u0483\u0788.int'");
    }

    protected void tearDown() throws Exception {
        qr1.close();
        qr2.close();
        qr3.close();
        qr4.close();
        qr5.close();
        qr6.close();
        qr7.close();
        qr8.close();
        super.tearDown();
    }

    public void testQR1NP() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR1NP: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr1.receive(2000);
                assertTrue("Received msg==null, i=" + i, msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR2NP() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR2NP: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR3NP() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR3NP: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            msg = (TextMessage) qr3.receive(2000);
            assertTrue("Received msg!=null", msg == null);

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR4NP() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR4NP: " + i);
                sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR5NP() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("HHLA_Aspect", "Asp");
            msg.setText("testQR5NP: 1");
            sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("HHLA_Aspect", "spec");
            msg.setText("testQR5NP: 2");
            sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("HHLA_Aspect", "Aspect");
            msg.setText("testQR5NP: 3");
            sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("HHLA_Aspect", "oops");
            msg.setText("testQR5NP: 4");
            sender.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            int cnt = 0;
            msg = (TextMessage) qr5.receive(2000);
            while (msg != null) {
                boolean ok = msg.getBooleanProperty("ok");
                assertTrue("Received msg is !ok", ok);
                cnt++;
                msg = (TextMessage) qr5.receive(2000);
            }
            assertTrue("cnt != 2", cnt == 2);
            msg = (TextMessage) receiver.receive(2000);
            while (msg != null) {
                msg = (TextMessage) receiver.receive(2000);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR1P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR1P: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr1.receive(2000);
                assertTrue("Received msg==null, i=" + i, msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR2P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR2P: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR3P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR3P: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            msg = (TextMessage) qr3.receive(2000);
            assertTrue("Received msg!=null", msg == null);

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) receiver.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR4P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("testQR4P: " + i);
                sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) qr4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            msg = (TextMessage) receiver.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR5P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("HHLA_Aspect", "Asp");
            msg.setText("testQR5NP: 1");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("HHLA_Aspect", "spec");
            msg.setText("testQR5NP: 2");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("HHLA_Aspect", "Aspect");
            msg.setText("testQR5NP: 3");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("HHLA_Aspect", "oops");
            msg.setText("testQR5NP: 4");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            int cnt = 0;
            msg = (TextMessage) qr5.receive(2000);
            while (msg != null) {
                boolean ok = msg.getBooleanProperty("ok");
                assertTrue("Received msg is !ok", ok);
                cnt++;
                msg = (TextMessage) qr5.receive(2000);
            }
            assertTrue("cnt != 2", cnt == 2);
            msg = (TextMessage) receiver.receive(2000);
            while (msg != null) {
                msg = (TextMessage) receiver.receive(2000);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR6P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("prop", "Nation/World");
            msg.setText("testQR6NP: 1");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("prop", "Nation/World ");
            msg.setText("testQR6NP: 2");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("prop", "Nation's World");
            msg.setText("testQR6NP: 3");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("prop", "Nation's' World");
            msg.setText("testQR6NP: 4");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            int cnt = 0;
            msg = (TextMessage) qr6.receive(2000);
            System.out.println("qr6: " + msg);
            while (msg != null) {
                boolean ok = msg.getBooleanProperty("ok");
                assertTrue("Received msg is !ok", ok);
                cnt++;
                msg = (TextMessage) qr6.receive(2000);
                System.out.println("qr6: " + msg);
            }
            assertTrue("cnt != 1", cnt == 1);
            cnt = 0;
            msg = (TextMessage) qr7.receive(2000);
            System.out.println("qr7: " + msg);
            while (msg != null) {
                boolean ok = msg.getBooleanProperty("ok");
                assertTrue("Received msg is !ok", ok);
                cnt++;
                msg = (TextMessage) qr7.receive(2000);
                System.out.println("qr7: " + msg);
            }
            assertTrue("cnt != 1", cnt == 1);
            msg = (TextMessage) receiver.receive(2000);
            while (msg != null) {
                msg = (TextMessage) receiver.receive(2000);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testQR8P() {
        try {
            TextMessage msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", true);
            msg.setStringProperty("vdmConnectionSource", "mbenson-ncm2.\u0183\u0286\u0382?\u0483\u0788.int");
            msg.setText("testQR8P: 1");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            msg = qs.createTextMessage();
            msg.setBooleanProperty("ok", false);
            msg.setStringProperty("vdmConnectionSource", "mbenson-ncm2.\u0183\u0286\u0382?\u0283\u0788.int");
            msg.setText("testQR8P: 2");
            sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            int cnt = 0;
            msg = (TextMessage) qr8.receive(2000);
            System.out.println("qr8: " + msg);
            while (msg != null) {
                boolean ok = msg.getBooleanProperty("ok");
                assertTrue("Received msg is !ok", ok);
                cnt++;
                msg = (TextMessage) qr8.receive(2000);
                System.out.println("qr8: " + msg);
            }
            assertTrue("cnt != 1", cnt == 1);
            msg = (TextMessage) receiver.receive(2000);
            while (msg != null) {
                msg = (TextMessage) receiver.receive(2000);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

