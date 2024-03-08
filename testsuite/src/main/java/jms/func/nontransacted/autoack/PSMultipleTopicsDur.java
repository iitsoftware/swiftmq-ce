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

package jms.func.nontransacted.autoack;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class PSMultipleTopicsDur extends SimpleConnectedPSTestCase {
    Topic t1 = null;
    Topic t2 = null;
    Topic t3 = null;
    Topic t4 = null;
    Topic t5 = null;
    TopicPublisher uipublisher = null;
    TopicPublisher tst1 = null;
    TopicPublisher tst2 = null;
    TopicPublisher tst3 = null;
    TopicPublisher tst4 = null;
    TopicPublisher tst5 = null;
    TopicSubscriber tsubt1 = null;
    TopicSubscriber tsubt2 = null;
    TopicSubscriber tsubt3 = null;
    TopicSubscriber tsubt4 = null;
    TopicSubscriber tsubt5 = null;

    public PSMultipleTopicsDur(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE, true);
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
        uipublisher = ts.createPublisher(null);
        tst1 = ts.createPublisher(t1);
        tst2 = ts.createPublisher(t2);
        tst3 = ts.createPublisher(t3);
        tst4 = ts.createPublisher(t4);
        tst5 = ts.createPublisher(t5);
        tsubt1 = ts.createDurableSubscriber(t1, "dur100");
        tsubt2 = ts.createDurableSubscriber(t2, "dur101");
        tsubt3 = ts.createDurableSubscriber(t3, "dur102");
        tsubt4 = ts.createDurableSubscriber(t4, "dur103");
        tsubt5 = ts.createDurableSubscriber(t5, "dur104");
    }

    public void testPSUnidentifiedDurNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uipublisher.publish(t1, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t2, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t3, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t4, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t5, msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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

    public void testPSIdentifiedDurNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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

    public void testPSUnidentifiedDurP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                uipublisher.publish(t1, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t2, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t3, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t4, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                uipublisher.publish(t5, msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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

    public void testPSIdentifiedDurP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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

    public void testPSSendReceiveDurNP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) subscriber.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }


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

    public void testPSSendReceiveDurP() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                tst1.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst2.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst3.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst4.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                tst5.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }


            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
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


            for (int i = 0; i < 10; i++) {
                msg = (TextMessage) subscriber.receive(2000);
                assertTrue("Received msg==null", msg != null);
            }


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

    protected void tearDown() throws Exception {
        uipublisher.close();
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
        ts.unsubscribe("dur100");
        ts.unsubscribe("dur101");
        ts.unsubscribe("dur102");
        ts.unsubscribe("dur103");
        ts.unsubscribe("dur104");
        deleteTopic("tt1");
        deleteTopic("tt2");
        deleteTopic("tt3");
        deleteTopic("tt4");
        deleteTopic("tt5");
        super.tearDown();
    }
}

