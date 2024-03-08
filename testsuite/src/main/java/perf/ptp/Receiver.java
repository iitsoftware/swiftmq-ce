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

package perf.ptp;

import jms.base.PTPTestCase;

import javax.jms.*;

public class Receiver extends PTPTestCase {
    QueueConnection qc = null;
    Queue queue = null;
    int n = 0;
    int size = 0;

    public Receiver(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        String qcfName = System.getProperty("jndi.qcf");
        assertNotNull("missing property 'jndi.qcf'", qcfName);
        qc = createQueueConnection(qcfName);
        String queueName = System.getProperty("jndi.queue");
        assertNotNull("missing property 'jndi.queue'", queueName);
        queue = getQueue(queueName);
        String s = System.getProperty("messages.number");
        assertNotNull("missing property 'messages.number'", s);
        n = Integer.parseInt(s);
        s = System.getProperty("messages.size");
        assertNotNull("missing property 'messages.size'", s);
        size = Integer.parseInt(s);
    }

    public void testReceive() {
        try {
            QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueReceiver qr = qs.createReceiver(queue);
            for (; ; ) {
                BytesMessage msg = (BytesMessage) qr.receive(2000);
                if (msg == null)
                    break;
            }
            qr.close();
            qs.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("testReceive failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        qc.close();
        super.tearDown();
    }
}

