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

package jms.funcunified.nontransacted.clientack;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.*;

public class PSSendSingleTopicOM extends SimpleConnectedUnifiedPSTestCase {
    Session ts2 = null;
    MessageConsumer consumer2 = null;
    Object sem = new Object();
    int cnt = 0;

    public PSSendSingleTopicOM(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.CLIENT_ACKNOWLEDGE);
        consumer.close();
        ts2 = tc.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer2 = ts2.createConsumer(topic);
    }

    public void testPSSendSingleTopicOMNP() {
        try {
            consumer2.setMessageListener(null);
            consumer2.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    synchronized (sem) {
                        cnt++;
                        TextMessage tm = (TextMessage) message;
                        try {
                            tm.acknowledge();
                        } catch (Exception jmse) {
                            failFast(jmse.toString());
                        }
                        if (cnt == 10) {
                            sem.notify();
                        }
                    }
                }
            });

            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            synchronized (sem) {
                if (cnt != 10) {
                    try {
                        sem.wait();
                    } catch (Exception ignored) {
                    }
                }
            }
            consumer2.setMessageListener(null);
            msg = (TextMessage) consumer2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    public void testPSSendSingleTopicOMP() {
        try {
            consumer2.setMessageListener(null);
            consumer2.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    synchronized (sem) {
                        cnt++;
                        TextMessage tm = (TextMessage) message;
                        try {
                            tm.acknowledge();
                        } catch (Exception jmse) {
                            failFast(jmse.toString());
                        }
                        if (cnt == 10) {
                            sem.notify();
                        }
                    }
                }
            });

            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < 10; i++) {
                msg.setText("Msg: " + i);
                producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }

            synchronized (sem) {
                if (cnt != 10) {
                    try {
                        sem.wait();
                    } catch (Exception ignored) {
                    }
                }
            }
            consumer2.setMessageListener(null);
            msg = (TextMessage) consumer2.receive(2000);
            assertTrue("Received msg!=null", msg == null);
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

