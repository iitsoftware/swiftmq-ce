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

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

public class SimpleConnectedUnifiedXAPS1MTestCase extends UnifiedXAPSTestCase
{
  public XAConnection tc = null;
  public XASession ts = null;
  public MessageProducer producer = null;
  public Destination destination = null;
  public MessageConsumer[] consumer = null;
  public XAResource xares = null;

  public SimpleConnectedUnifiedXAPS1MTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(3);
  }

  protected void setUp(int subscribers) throws Exception
  {
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createXAConnection(tcfName, "XAPSTest-" + System.currentTimeMillis());
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    destination = getTopic(topicName);
    ts = tc.createXASession();
    producer = ts.getSession().createProducer(destination);
    consumer = new MessageConsumer[subscribers];
    for (int i = 0; i < subscribers; i++)
    {
      consumer[i] = ts.getSession().createDurableSubscriber(getTopic(topicName), "dur" + i);
    }
    xares = ts.getXAResource();
    tc.start();
  }

  protected void tearDown() throws Exception
  {
    producer.close();
    if (consumer != null)
    {
      for (int i = 0; i < consumer.length; i++)
      {
        consumer[i].close();
        ts.getSession().unsubscribe("dur" + i);
      }
    }
    ts.close();
    tc.close();
    tc = null;
    ts = null;
    producer = null;
    destination = null;
    consumer = null;
    xares = null;
    super.tearDown();
  }

}

