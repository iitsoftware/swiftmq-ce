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

public class MultisessionConnectedXAPTPTestCase extends XAPTPTestCase {
    public XAQueueConnection qc = null;
    public XAQueueSession[] sessions = null;
    public Queue queue = null;
    public QueueSender[] addSender = null;
    public QueueReceiver[] addReceiver = null;

    public MultisessionConnectedXAPTPTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(1);
    }

    protected void setUp(int nSessions) throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createXAQueueConnection(qcfName);
        String queueName = System.getProperty("jndi.queue");
        assertNotNull("missing property 'jndi.queue'", queueName);
        queue = getQueue(queueName);
        sessions = new XAQueueSession[nSessions];
        for (int i = 0; i < nSessions; i++)
            sessions[i] = qc.createXAQueueSession();
        qc.start();
    }

    protected void tearDown() throws Exception {
        for (int i = 0; i < sessions.length; i++)
            sessions[i].close();
        qc.close();
        super.tearDown();
    }

}

