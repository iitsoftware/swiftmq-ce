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

import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.transaction.xa.XAResource;

public class SimpleConnectedXAPTPTestCase extends XAPTPTestCase
{
  public XAQueueConnection qc = null;
  public XAQueueSession qs = null;
  public QueueSender sender = null;
  public QueueReceiver receiver = null;
  public Queue queue = null;
  public XAResource xares = null;
  public QueueSender[] addSender = null;
  public QueueReceiver[] addReceiver = null;
  public boolean createSender = true;
  public boolean createReceiver = true;
  public boolean createAddSender = true;
  public boolean createAddReceiver = true;

  public SimpleConnectedXAPTPTestCase(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(0);
  }

  protected void setUp(int additionalQueues) throws Exception
  {
    String qcfName = System.getProperty("jndi.qcf");
    assertNotNull("missing property 'jndi.qcf'", qcfName);
    qc = createXAQueueConnection(qcfName);
    String queueName = System.getProperty("jndi.queue");
    assertNotNull("missing property 'jndi.queue'", queueName);
    queue = getQueue(queueName);
    qs = qc.createXAQueueSession();
    xares = qs.getXAResource();
    if (createSender)
      sender = qs.getQueueSession().createSender(queue);
    if (createReceiver)
      receiver = qs.getQueueSession().createReceiver(queue);
    if (additionalQueues > 0)
    {
      if (createAddSender)
      {
        addSender = new QueueSender[additionalQueues];
        for (int i = 0; i < additionalQueues; i++)
        {
          addSender[i] = qs.getQueueSession().createSender(getQueue("t" + i + "@router"));
        }
      }
      if (createAddReceiver)
      {
        addReceiver = new QueueReceiver[additionalQueues];
        for (int i = 0; i < additionalQueues; i++)
        {
          addReceiver[i] = qs.getQueueSession().createReceiver(getQueue("t" + i + "@router"));
        }
      }
    }
    qc.start();
  }

  protected void tearDown() throws Exception
  {
    qc.close();
    qc = null;
    qs = null;
    sender = null;
    receiver = null;
    queue = null;
    xares = null;
    addSender = null;
    addReceiver = null;
    super.tearDown();
  }

}

