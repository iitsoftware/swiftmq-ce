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

public class SimpleConnectedPTPClusterTestCase extends SimpleConnectedPTPTestCase
{
  public SimpleConnectedPTPClusterTestCase(String name)
  {
    super(name);
  }

  protected void setUp(boolean transacted, int ackMode, boolean createSender, boolean createReceiver) throws Exception
  {
    String qcfName = System.getProperty("jndi.qcf");
    assertNotNull("missing property 'jndi.qcf'", qcfName);
    qc = createQueueConnection(qcfName);
    String queueName = System.getProperty("jndi.cluster.queue");
    assertNotNull("missing property 'jndi.queue'", queueName);
    queue = getQueue(queueName);
    beforeCreateSession();
    qs = qc.createQueueSession(transacted, ackMode);
    afterCreateSession();
    if (createSender)
    {
      beforeCreateSender();
      sender = qs.createSender(queue);
      afterCreateSender();
    }
    if (createReceiver)
    {
      beforeCreateReceiver();
      receiver = qs.createReceiver(queue);
      afterCreateReceiver();
    }
    qc.start();
  }
}
