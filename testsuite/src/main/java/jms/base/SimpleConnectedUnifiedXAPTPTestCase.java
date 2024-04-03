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

public class SimpleConnectedUnifiedXAPTPTestCase extends UnifiedXAPTPTestCase {
    public XAConnection qc = null;
    public XASession qs = null;
    public MessageProducer producer = null;
    public MessageConsumer consumer = null;
    public Destination destination = null;
    public XAResource xares = null;
    public MessageProducer[] addProducer = null;
    public MessageConsumer[] addConsumer = null;

    public SimpleConnectedUnifiedXAPTPTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(0);
    }

    protected void setUp(int additionalQueues) throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createXAConnection(qcfName);
        String queueName = System.getProperty("jndi.queue");
        assertNotNull("missing property 'jndi.queue'", queueName);
        destination = getQueue(queueName);
        qs = qc.createXASession();
        xares = qs.getXAResource();
        producer = qs.getSession().createProducer(destination);
        consumer = qs.getSession().createConsumer(destination);
        if (additionalQueues > 0) {
            addProducer = new MessageProducer[additionalQueues];
            for (int i = 0; i < additionalQueues; i++) {
                addProducer[i] = qs.getSession().createProducer(getQueue("t" + i + "@router"));
            }
            addConsumer = new MessageConsumer[additionalQueues];
            for (int i = 0; i < additionalQueues; i++) {
                addConsumer[i] = qs.getSession().createConsumer(getQueue("t" + i + "@router"));
            }
        }
        qc.start();
    }

    protected void tearDown() throws Exception {
        qc.close();
        qc = null;
        qs = null;
        producer = null;
        consumer = null;
        destination = null;
        xares = null;
        addProducer = null;
        addConsumer = null;
        super.tearDown();
    }

}

