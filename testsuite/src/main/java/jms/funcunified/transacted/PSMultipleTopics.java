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

package jms.funcunified.transacted;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.*;

public class PSMultipleTopics extends SimpleConnectedUnifiedPSTestCase {
    Topic t1 = null;
    Topic t2 = null;
    Topic t3 = null;
    Topic t4 = null;
    Topic t5 = null;
    MessageProducer uiproducer = null;
    MessageProducer tst1 = null;
    MessageProducer tst2 = null;
    MessageProducer tst3 = null;
    MessageProducer tst4 = null;
    MessageProducer tst5 = null;
    MessageConsumer tsubt1 = null;
    MessageConsumer tsubt2 = null;
    MessageConsumer tsubt3 = null;
    MessageConsumer tsubt4 = null;
    MessageConsumer tsubt5 = null;

    public PSMultipleTopics(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(true, Session.CLIENT_ACKNOWLEDGE);
        createTopic("tt1");
        createTopic("tt2");
        createTopic("tt3");
        createTopic("tt4");
        createTopic("tt5");
        t1 = (Topic) ctx.lookup("tt1");
        t2 = (Topic) ctx.lookup("tt2");
        t3 = (Topic) ctx.lookup("tt3");
        t4 = (Topic) ctx.lookup("tt4");
        t5 = (Topic) ctx.lookup("tt5");
        uiproducer = ts.createProducer(null);
        tst1 = ts.createProducer(t1);
        tst2 = ts.createProducer(t2);
        tst3 = ts.createProducer(t3);
        tst4 = ts.createProducer(t4);
        tst5 = ts.createProducer(t5);
        tsubt1 = ts.createConsumer(t1);
        tsubt2 = ts.createConsumer(t2);
        tsubt3 = ts.createConsumer(t3);
        tsubt4 = ts.createConsumer(t4);
        tsubt5 = ts.createConsumer(t5);
    }

    public void testPSCommitUnidentifiedNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uiproducer.send(t1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();
            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSCommitIdentifiedNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();
            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSCommitUnidentifiedP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uiproducer.send(t1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uiproducer.send(t5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();
            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSCommitIdentifiedP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();
            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSCommitSendReceiveNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) consumer.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();

            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSCommitSendReceiveP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) consumer.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.commit();

            msg = (TextMessage) tsubt1.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt3.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt4.receive(2000);
            assertTrue("Received msg!=null", msg == null);
            msg = (TextMessage) tsubt5.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSRollbackSendReceiveNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.rollback();

            msg = (TextMessage) consumer.receive(2000);
            assertTrue("Received msg!=null", msg == null);


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }
            ts.commit();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSRollbackSendReceiveP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            ts.commit();

            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }
            ts.rollback();

            msg = (TextMessage) consumer.receive(2000);
            assertTrue("Received msg!=null", msg == null);


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt1.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt2.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt3.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt4.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }

            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) tsubt5.receive(2000);
                assertTrue("Received msg==null", msg != null);
                boolean redelivered = msg.getJMSRedelivered();
                assertTrue("Msg not marked as redelivered", redelivered);
                int cnt = msg.getIntProperty("JMSXDeliveryCount");
                assertTrue("Invalid delivery count: " + cnt, cnt == 2);
            }
            ts.commit();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        uiproducer.close();
        tst1.close();
        tst2.close();
        tst3.close();
        tst4.close();
        tst5.close();
        tsubt1.close();
        tsubt2.close();
        tsubt3.close();
        tsubt4.close();
        tsubt5.close();
        deleteTopic("tt1");
        deleteTopic("tt2");
        deleteTopic("tt3");
        deleteTopic("tt4");
        deleteTopic("tt5");
        super.tearDown();
    }
}

