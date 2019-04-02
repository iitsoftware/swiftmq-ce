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

package jms.ccunified.transacted.psdur;

import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.UnifiedPSTestCase;

import javax.jms.*;
import javax.naming.InitialContext;

public class Tester extends UnifiedPSTestCase
{
  InitialContext ctx = null;
  Connection tc = null;
  Connection tc1 = null;
  Session qs = null;
  MessageProducer producer = null;
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
    tc = createConnection(tcfName, "jms.cc-test", false);
    String topicName = System.getProperty("jndi.topic");
    assertNotNull("missing property 'jndi.topic'", topicName);
    topic = getTopic(topicName);
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 10; i++)
    {
      Session qs = tc.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      qs.setMessageListener(new Listener(qs, i));
      pool.addServerSession(new ServerSessionImpl(pool, qs));
    }
    cc = tc.createDurableConnectionConsumer(topic, "transacted", null, pool, 5);
    tc1 = createConnection(tcfName, true);
    qs = tc1.createSession(false, Session.AUTO_ACKNOWLEDGE);
    producer = qs.createProducer(topic);
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
        producer.send(msg);
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
    Session session;
    int id;

    public Listener(Session session, int id)
    {
      this.session = session;
      this.id = id;
    }

    public void onMessage(Message msg)
    {
      try
      {
        session.commit();
      } catch (JMSException e)
      {
        e.printStackTrace();
      }
      inc();
    }
  }
}