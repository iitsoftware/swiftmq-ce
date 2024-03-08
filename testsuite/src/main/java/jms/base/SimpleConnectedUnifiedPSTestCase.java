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

public class SimpleConnectedUnifiedPSTestCase extends UnifiedPSTestCase {
    public Connection tc = null;
    public Session ts = null;
    public MessageProducer producer = null;
    public MessageConsumer consumer = null;
    public Topic topic = null;
    boolean isDurable = false;

    public SimpleConnectedUnifiedPSTestCase(String name) {
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
        tc = createConnection(tcfName, isDurable ? ("PSTest-" + nextId()) : null);
        String topicName = System.getProperty("jndi.topic");
        assertNotNull("missing property 'jndi.topic'", topicName);
        topic = getTopic(topicName);
        ts = tc.createSession(transacted, ackMode);
        producer = ts.createProducer(topic);
        consumer = isDurable ? ts.createDurableSubscriber(topic, "dur") : ts.createConsumer(topic);
        tc.start();
    }

    protected void tearDown() throws Exception {
        consumer.close();
        producer.close();
        if (isDurable)
            ts.unsubscribe("dur");
        ts.close();
        tc.close();
        tc = null;
        ts = null;
        producer = null;
        consumer = null;
        topic = null;
        super.tearDown();
    }

}

