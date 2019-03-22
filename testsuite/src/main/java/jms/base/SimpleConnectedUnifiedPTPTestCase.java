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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class SimpleConnectedUnifiedPTPTestCase extends UnifiedPTPTestCase
{
  public Connection qc = null;
  public Session qs = null;
  public MessageProducer producer = null;
  public MessageConsumer consumer = null;
  public Queue queue = null;

  public SimpleConnectedUnifiedPTPTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
  }

  protected void setUp(boolean transacted, int ackMode) throws Exception
  {
    String qcfName = System.getProperty("jndi.qcf");
    assertNotNull("missing property 'jndi.qcf'", qcfName);
    qc = createConnection(qcfName);
    String queueName = System.getProperty("jndi.queue");
    assertNotNull("missing property 'jndi.queue'", queueName);
    queue = getQueue(queueName);
    qs = qc.createSession(transacted, ackMode);
    consumer = qs.createConsumer(queue);
    producer = qs.createProducer(queue);
    qc.start();
  }

  protected void tearDown() throws Exception
  {
    qc.close();
    qc = null;
    qs = null;
    producer = null;
    consumer = null;
    queue = null;
    super.tearDown();
  }

}

