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

package jms.cc.xa.multiple.ptp;

import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.XAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.transaction.xa.*;

public class MDBForwarder extends XAPTPTestCase
{
  InitialContext ctx = null;
  XAQueueConnection qc = null;
  Queue queueListen = null;
  Queue queueForward = null;
  ConnectionConsumer cc = null;
  ServerSessionPoolImpl pool = null;
  int nMsgs = 0;
  String listenQueue = null;
  String forwardQueue = null;

  public MDBForwarder(String name, String listenQueue, String forwardQueue)
  {
    super(name);
    this.listenQueue = listenQueue;
    this.forwardQueue = forwardQueue;
  }

  protected void setUp() throws Exception
  {
    String qcfName = System.getProperty("jndi.qcf");
    assertNotNull("missing property 'jndi.qcf'", qcfName);
    qc = createXAQueueConnection(qcfName, false);
    queueListen = getQueue(listenQueue);
    queueForward = getQueue(forwardQueue);
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 5; i++)
    {
      XAQueueSession qs = qc.createXAQueueSession();
      qs.setMessageListener(new Listener(qs, i));
      pool.addServerSession(new ServerSessionImpl(pool, qs));
    }
    cc = qc.createConnectionConsumer(queueListen, null, pool, 5);
  }

  synchronized void inc()
  {
    nMsgs++;
    if (nMsgs == 1000)
      notify();
  }

  public void test()
  {
    try
    {
      synchronized (this)
      {
        qc.start();
        try
        {
          wait();
        } catch (InterruptedException e)
        {
        }
      }
    } catch (Exception e)
    {
      fail("Test failed: " + e.toString());
    }
  }

  protected void tearDown() throws Exception
  {
    qc.close();
    super.tearDown();
  }

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
        XAResource xares1 = session.getXAResource();  // done during JTA enlistment
        Xid xid = new XidImpl();                      // done during JTA enlistment
        xares1.start(xid, XAResource.TMNOFLAGS);       // done during JTA enlistment

        // Start: MDB's onMessage
        // Must create a distinct session for every forwarded message,
        // since a session is single-threaded!
        System.out.println(listenQueue + " received: " + ((TextMessage) msg).getText() + " forward to: " + forwardQueue);
        XAQueueSession qs = qc.createXAQueueSession();
        QueueSender sender = qs.getQueueSession().createSender(queueForward);
        XAResource xares2 = qs.getXAResource();       // done during JTA enlistment
        xares2.start(xid, XAResource.TMJOIN);       // done during JTA enlistment
        sender.send(msg);
        sender.close();
        // End: MDB's onMessage

        // Problem: the XAQueueSession to forward couldn't be closed
        // until the transaction has been committed, but this is done outside
        // the MDB's onMessage, so there is actually no way to use XA here!

        xares1.end(xid, XAResource.TMSUCCESS);        // done by the JTA
        xares2.end(xid, XAResource.TMSUCCESS);        // done by the JTA
        xares1.prepare(xid);                         // done by the JTA
        xares1.commit(xid, false);                    // done by the JTA

        // close the XASession
        qs.close();
      } catch (Exception e)
      {
        e.printStackTrace();
      }
      inc();
    }
  }
}
