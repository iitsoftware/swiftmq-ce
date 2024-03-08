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
import javax.transaction.xa.XAResource;

public class SimpleConnectedUnifiedXAPSTestCase extends UnifiedXAPSTestCase {
    public XAConnection tc = null;
    public XASession ts = null;
    public MessageProducer producer = null;
    public MessageConsumer consumer = null;
    public Destination destination = null;
    public MessageProducer[] addProducer = null;
    public MessageConsumer[] addConsumer = null;
    public XAResource xares = null;

    public SimpleConnectedUnifiedXAPSTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(0);
    }

    protected void setUp(int additionalTopics) throws Exception {
        String tcfName = System.getProperty("jndi.tcf");
        assertNotNull("missing property 'jndi.tcf'", tcfName);
        tc = createXAConnection(tcfName, "XAPSTest-" + System.currentTimeMillis());
        String topicName = System.getProperty("jndi.topic");
        assertNotNull("missing property 'jndi.topic'", topicName);
        destination = getTopic(topicName);
        ts = tc.createXASession();
        producer = ts.getSession().createProducer(destination);
        consumer = ts.getSession().createDurableSubscriber((Topic) destination, "dur");
        if (additionalTopics > 0) {
            addProducer = new MessageProducer[additionalTopics];
            for (int i = 0; i < additionalTopics; i++) {
                addProducer[i] = ts.getSession().createProducer(getTopic(topicName + i));
            }
            addConsumer = new MessageConsumer[additionalTopics];
            for (int i = 0; i < additionalTopics; i++) {
                addConsumer[i] = ts.getSession().createDurableSubscriber(getTopic(topicName + i), "dur" + i);
            }
        }
        xares = ts.getXAResource();
        tc.start();
    }

    protected void tearDown() throws Exception {
        consumer.close();
        producer.close();
        if (addProducer != null) {
            for (int i = 0; i < addProducer.length; i++) {
                addProducer[i].close();
            }
        }
        if (addConsumer != null) {
            for (int i = 0; i < addConsumer.length; i++) {
                addConsumer[i].close();
                ts.getSession().unsubscribe("dur" + i);
            }
        }
        ts.getSession().unsubscribe("dur");
        ts.close();
        tc.close();
        tc = null;
        ts = null;
        producer = null;
        consumer = null;
        destination = null;
        addProducer = null;
        addConsumer = null;
        xares = null;
        super.tearDown();
    }

}

