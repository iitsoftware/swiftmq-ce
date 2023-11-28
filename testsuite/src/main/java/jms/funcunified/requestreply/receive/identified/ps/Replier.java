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

package jms.funcunified.requestreply.receive.identified.ps;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;

public class Replier extends SimpleConnectedUnifiedPSTestCase {

    public Replier(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void testReply() {
        try {
            for (int i = 0; i < 10000; i++) {
                TextMessage msg = (TextMessage) consumer.receive();
                MessageProducer replyProducer = ts.createProducer((TemporaryTopic) msg.getJMSReplyTo());
                msg.clearBody();
                msg.setText("Re: " + msg.getText());
                replyProducer.send(msg);
                replyProducer.close();
            }
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }
}

