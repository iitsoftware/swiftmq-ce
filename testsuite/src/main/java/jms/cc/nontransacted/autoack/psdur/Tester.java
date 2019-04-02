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

package jms.cc.nontransacted.autoack.psdur;

import jms.base.PSTestCase;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;

import javax.jms.*;
import javax.naming.InitialContext;

public class Tester extends PSTestCase
{
  InitialContext ctx = null;
  TopicConnection tc = null;
  TopicConnection tc1 = null;
  TopicSession qs = null;
  TopicPublisher publisher = null;
  Topic topic = null;
  ConnectionConsumer cc = null;
  ServerSessionPoolImpl pool = null;
  int nMsgs = 0;

  public Tester(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    String tcfName = System.getProperty("jndi.tcf");
    assertNotNull("missing property 'jndi.tcf'", tcfName);
    tc = createTopicConnection(tcfName, "jms-cc-test", false);
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 10; i++)
    {
      TopicSession qs = tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      qs.setMessageListener(new Listener(i));
      pool.addServerSession(new ServerSessionImpl(pool, qs));
    }
    cc = tc.createDurableConnectionConsumer(topic, "autoack", null, pool, 5);
    tc1 = createTopicConnection(tcfName, true);
    qs = tc1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    publisher = qs.createPublisher(topic);
  }

  synchronized void inc()
  {
    nMsgs++;
    if (nMsgs == 10000)
      notify();
  }

  public void test()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10000; i++)
      {
        msg.setText("Msg: " + (i + 1));
        publisher.publish(msg);
      }

      tc.start();
      synchronized (this)
      {
        try
        {
          wait();
        } catch (InterruptedException e)
        {
        }
      }
    } catch (Exception e)
    {
      failFast("Test failed: " + e.toString());
    }
  }

  protected void tearDown() throws Exception
  {
    tc.close();
    tc1.close();
    super.tearDown();
  }

  private class Listener implements MessageListener
  {
    int id;

    public Listener(int id)
    {
      this.id = id;
    }

    public void onMessage(Message msg)
    {
      inc();
    }
  }
}
