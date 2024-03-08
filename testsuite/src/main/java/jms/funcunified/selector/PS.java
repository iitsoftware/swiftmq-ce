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

package jms.funcunified.selector;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.*;

public class PS extends SimpleConnectedUnifiedPSTestCase {
    MessageConsumer tsub1 = null;
    MessageConsumer tsub2 = null;
    MessageConsumer tsub3 = null;
    MessageConsumer tsub4 = null;

    public PS(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        tsub1 = ts.createConsumer(topic, "Int1 between 90 and 120 and time >= " + System.currentTimeMillis(), false);
        tsub2 = ts.createConsumer(topic, "D1 > 1.0 and D1 < 1000.0", false);
        tsub3 = ts.createConsumer(topic, "Int2=200 and Int1=400 and S1 like 'Moin%'", false);
        tsub4 = ts.createConsumer(topic, "D1 > 1.0 and D1 < 1000.0 and S1 in ('Moin', 'MOIN', 'Moin Moin')", false);
    }

    protected void tearDown() throws Exception {
        tsub1.close();
        tsub2.close();
        tsub3.close();
        tsub4.close();
        super.tearDown();
    }

    public void testTS1NP() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS2NP() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS3NP() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            msg = (TextMessage) tsub3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS4NP() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS1P() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS2P() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS3P() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            msg = (TextMessage) tsub3.receive(2000);
            assertTrue("Received msg!=null", msg == null);

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testTS4P() {
        try {
            TextMessage msg = ts.createTextMessage();
            msg.setIntProperty("Int1", 100);
            msg.setIntProperty("Int2", 200);
            msg.setIntProperty("Int3", 300);
            msg.setStringProperty("S1", "Moin Moin");
            msg.setLongProperty("time", System.currentTimeMillis());
            msg.setDoubleProperty("D1", 88.99);
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsub4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

