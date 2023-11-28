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

public class SimpleConnectedUnifiedXAMixedTestCase extends UnifiedXAMixedTestCase {
    public XAConnection tc = null;
    public XASession ts = null;
    public MessageProducer[] addProducer = null;
    public MessageConsumer[] addConsumer = null;
    public XAResource xares = null;
    int additionalQueue = 0;
    int additionalTopic = 0;

    public SimpleConnectedUnifiedXAMixedTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        setUp(5, 5);
    }

    protected void setUp(int additionalQueue, int additionalTopic) throws Exception {
        this.additionalQueue = additionalQueue;
        this.additionalTopic = additionalTopic;
        String tcfName = System.getProperty("jndi.tcf");
        assertNotNull("missing property 'jndi.tcf'", tcfName);
        tc = createXAConnection(tcfName, "XAPSTest-" + System.currentTimeMillis());
        ts = tc.createXASession();
        addProducer = new MessageProducer[additionalQueue + additionalTopic];
        addConsumer = new MessageConsumer[additionalQueue + additionalTopic];
        for (int i = 0; i < additionalTopic; i++) {
            try {
                createTopic("tt" + i);
            } catch (Exception e) {
            }
            addProducer[i] = ts.getSession().createProducer(getDestination("tt" + i));
        }
        for (int i = 0; i < additionalQueue; i++) {
            try {
                createQueue("t" + i);
            } catch (Exception e) {
            }
            addProducer[additionalTopic + i] = ts.getSession().createProducer(getDestination("t" + i + "@router"));
        }
        for (int i = 0; i < additionalTopic; i++) {
            addConsumer[i] = ts.getSession().createDurableSubscriber((Topic) getDestination("tt" + i), "dur" + i);
        }
        for (int i = 0; i < additionalQueue; i++) {
            addConsumer[additionalTopic + i] = ts.getSession().createConsumer(getDestination("t" + i + "@router"));
        }
        xares = ts.getXAResource();
        tc.start();
    }

    protected void tearDown() throws Exception {
        for (int i = 0; i < additionalTopic; i++) {
            ts.getSession().unsubscribe("dur" + i);
        }
        ts.close();
        tc.close();
        tc = null;
        ts = null;
        addProducer = null;
        addConsumer = null;
        xares = null;
        super.tearDown();
    }

}

