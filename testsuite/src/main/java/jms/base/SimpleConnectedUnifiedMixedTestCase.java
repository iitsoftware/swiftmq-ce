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

package jms.base;

import javax.jms.*;

public class SimpleConnectedUnifiedMixedTestCase extends UnifiedMixedTestCase {
    public Connection tc = null;
    public Session ts = null;
    public MessageProducer producerQueue = null;
    public MessageConsumer consumerQueue = null;
    public MessageProducer producerTopic = null;
    public MessageConsumer consumerTopic = null;
    public Destination destQueue = null;
    public Destination destTopic = null;
    boolean isDurable = false;

    public SimpleConnectedUnifiedMixedTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected void setUp(boolean transacted, int ackMode) throws Exception {
        setUp(transacted, ackMode, false);
    }

    protected void setUp(boolean transacted, int ackMode, boolean isDurable) throws Exception {
        this.isDurable = isDurable;
        String tcfName = System.getProperty("jndi.tcf");
        assertNotNull("missing property 'jndi.tcf'", tcfName);
        tc = createConnection(tcfName, isDurable ? ("PSTest-" + System.currentTimeMillis()) : null);
        String topicName = System.getProperty("jndi.topic");
        assertNotNull("missing property 'jndi.topic'", topicName);
        destTopic = getDestination(topicName);
        ts = tc.createSession(transacted, ackMode);
        producerTopic = ts.createProducer(destTopic);
        consumerTopic = isDurable ? ts.createDurableSubscriber((Topic) destTopic, "dur") : ts.createConsumer(destTopic);
        String queueName = System.getProperty("jndi.queue");
        assertNotNull("missing property 'jndi.queue'", queueName);
        destQueue = getDestination(queueName);
        consumerQueue = ts.createConsumer(destQueue);
        producerQueue = ts.createProducer(destQueue);
        tc.start();
    }

    protected void tearDown() throws Exception {
        consumerQueue.close();
        producerQueue.close();
        consumerTopic.close();
        producerTopic.close();
        if (isDurable)
            ts.unsubscribe("dur");
        ts.close();
        tc.close();
        tc = null;
        ts = null;
        producerQueue = null;
        consumerQueue = null;
        producerTopic = null;
        consumerTopic = null;
        destQueue = null;
        destTopic = null;
        super.tearDown();
    }

}

