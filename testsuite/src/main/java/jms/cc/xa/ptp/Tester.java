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

package jms.cc.xa.ptp;

import jms.base.*;
import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.*;

public class Tester extends XAPTPTestCase
{
  InitialContext ctx = null;
  XAQueueConnection qc = null;
  XAQueueConnection qc1 = null;
  XAQueueSession qs = null;
  QueueSender sender = null;
  QueueReceiver receiver = null;
  Queue queue = null;
  ConnectionConsumer cc = null;
  ServerSessionPoolImpl pool = null;
  int nMsgs = 0;

  public Tester(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    String qcfName = System.getProperty("jndi.qcf");
    assertNotNull("missing property 'jndi.qcf'", qcfName);
    qc = createXAQueueConnection(qcfName, false);
    String queueName = System.getProperty("jndi.queue");
    assertNotNull("missing property 'jndi.queue'", queueName);
    queue = getQueue(queueName);
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 50; i++)
    {
      XAQueueSession qs = qc.createXAQueueSession();
      qs.setMessageListener(new Listener(qs, i));
      pool.addServerSession(new ServerSessionImpl(pool, qs));
    }
    cc = qc.createConnectionConsumer(queue, null, pool, 5);
    qc1 = createXAQueueConnection(qcfName, true);
    qs = qc1.createXAQueueSession();
    sender = qs.getQueueSession().createSender(queue);
    receiver = qs.getQueueSession().createReceiver(queue);
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
        sender.send(msg);
        qs.getQueueSession().commit();
      }

      qc.start();
      synchronized (this)
      {
        try
        {
          wait();
        } catch (InterruptedException e)
        {
        }
      }
      msg = (TextMessage) receiver.receive(2000);
      assertTrue("Msg != null", msg == null);
    } catch (Exception e)
    {
      failFast("Test failed: " + e.toString());
    }
  }

  protected void tearDown() throws Exception
  {
    qc.close();
    qc1.close();
    super.tearDown();
  }

  // This Listener is the *proxy* for XA handling an app server has to provide.
  // With onMessage it gets the XAResource from the XA session, creates a unique
  // Xid and starts the XA transaction. Thereafter onMessage of the MDB is called.
  // After return, the XA transaction ends and, dependent on what the MDB has votet,
  // prepare/rollback resp. prepare/commit is called.
  private class Listener implements MessageListener
  {
    XAQueueSession session;
    int id;

    public Listener(XAQueueSession session, int id)
    {
      this.session = session;
      this.id = id;
    }

    public void onMessage(Message msg)
    {
      try
      {
        XAResource xares = session.getXAResource();
        Xid xid = new XidImpl();
        xares.start(xid, XAResource.TMNOFLAGS);

        // Here will the MDB's onMessage be called!

        xares.end(xid, XAResource.TMSUCCESS);
        xares.prepare(xid);
        xares.commit(xid, false);
      } catch (Exception e)
      {
        e.printStackTrace();
      }
      inc();
    }
  }
}
