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

package load.requestreply.identified.multitempqueues.multisessions.timeout.ptp;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Replier extends SimpleConnectedPTPTestCase {
    Object sem = new Object();
    int cnt = 0;
    int n = 0;
    int m = 0;
    int max = 0;

    public Replier(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        n = Integer.parseInt(System.getProperty("load.requestreply.requestors", "200"));
        m = Integer.parseInt(System.getProperty("load.requestreply.msgs", "1000"));
        max = n * m;
    }

    public void testReply() {
        try {
            receiver.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        TextMessage msg = (TextMessage) message;
                        QueueSession session = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        QueueSender replySender = session.createSender((TemporaryQueue) msg.getJMSReplyTo());
                        msg.clearBody();
                        msg.setText("Re: " + msg.getText());
                        replySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500000);
                        try {
                            replySender.close();
                        } catch (JMSException e) {
                        }
                        session.close();
                        cnt++;
                        if (cnt % 100 == 0)
                            System.out.println(cnt + " so far...");
                        if (cnt == max) {
                            synchronized (sem) {
                                sem.notify();
                            }
                        }
                    } catch (Exception e1) {
                        fail("onMessage failed: " + e1);
                    }
                }
            });
            try {
                synchronized (sem) {
                    sem.wait();
                }
            } catch (Exception ignored) {
            }
        } catch (Exception e) {
            fail("test failed: " + e);
        }
    }
}

