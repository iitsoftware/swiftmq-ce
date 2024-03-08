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

package jms.ha.persistent.ps.durable.transacted.commit.single;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class Sender extends SimpleConnectedPSTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    TopicSession s1 = null;
    TopicSession s2 = null;
    TopicSession s3 = null;
    TopicPublisher publisher1 = null;
    TopicPublisher publisher2 = null;
    TopicPublisher publisher3 = null;

    public Sender(String name) {
        super(name);
    }

    protected void beforeCreateSession() throws Exception {
        s1 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
        s2 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
        s3 = tc.createTopicSession(true, Session.AUTO_ACKNOWLEDGE);
    }

    protected void afterCreateSession() throws Exception {
        s1.close();
        s2.close();
        s3.close();
    }

    protected void beforeCreateSender() throws Exception {
        publisher1 = ts.createPublisher(topic);
        publisher2 = ts.createPublisher(topic);
        publisher3 = ts.createPublisher(topic);
    }

    protected void afterCreateSender() throws Exception {
        publisher1.close();
        publisher2.close();
        publisher3.close();
    }

    protected void setUp() throws Exception {
        setUp(true, Session.AUTO_ACKNOWLEDGE, true, true, false);
        pause(20000);
    }

    public void send() {
        try {
            TextMessage msg = ts.createTextMessage();
            for (int i = 0; i < nMsgs; i++) {
                msg.setIntProperty("no", i);
                msg.setText("Msg: " + i);
                publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                if ((i + 1) % 10 == 0)
                    ts.commit();
            }

        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        s1 = null;
        s2 = null;
        s3 = null;
        publisher1 = null;
        publisher2 = null;
        publisher3 = null;
        super.tearDown();
    }
}

