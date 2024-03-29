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

package jms.func.requestreply.receive.unidentified.ps;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;

public class Replier extends SimpleConnectedPSTestCase {
    TopicPublisher replyPublisher = null;

    public Replier(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
        replyPublisher = ts.createPublisher(null);
    }

    public void testReply() {
        try {
            for (int i = 0; i < 10000; i++) {
                TextMessage msg = (TextMessage) subscriber.receive();
                msg.clearBody();
                msg.setText("Re: " + msg.getText());
                replyPublisher.publish((TemporaryTopic) msg.getJMSReplyTo(), msg);
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

