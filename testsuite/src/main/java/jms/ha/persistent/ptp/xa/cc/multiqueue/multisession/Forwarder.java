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

package jms.ha.persistent.ptp.xa.cc.multiqueue.multisession;

import jms.base.MsgNoVerifier;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;
import com.swiftmq.tools.concurrent.Semaphore;

import javax.jms.ConnectionConsumer;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.QueueSender;
import javax.jms.XAQueueSession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Forwarder extends SimpleConnectedXAPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  MsgNoVerifier verifier = null;
  ConnectionConsumer cc = null;
  ServerSessionPoolImpl pool = null;
  volatile Semaphore sem = null;
  Exception exception = null;
  int n = 0;
  String sourceQueueName = null;
  String targetQueueName = null;

  public Forwarder(String name, String sourceQueueName, String targetQueueName)
  {
    super(name);
    this.sourceQueueName = sourceQueueName;
    this.targetQueueName = targetQueueName;
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    sender.close();
    receiver.close();
    qs.close();
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 10; i++)
    {
      XAQueueSession session = qc.createXAQueueSession();
      session.setMessageListener(new Listener(session, session.getQueueSession().createSender(getQueue(targetQueueName))));
      pool.addServerSession(new ServerSessionImpl(pool, session));
    }
    cc = qc.createConnectionConsumer(getQueue(sourceQueueName), null, pool, 5);
    verifier = new MsgNoVerifier(this, nMsgs, "no");
    verifier.setCheckSequence(false);
  }

  synchronized void inc()
  {
    n++;
    if (n == nMsgs)
      sem.notifySingleWaiter();
  }

  public void forward()
  {
    sem = new Semaphore();
    sem.waitHere();
    if (exception != null)
      fail("failed: " + exception);
    try
    {
      verifier.verify();
    } catch (Exception e)
    {
      fail("failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    cc.close();
    verifier = null;
    cc = null;
    pool = null;
    sem = null;
    exception = null;
    super.tearDown();
  }

  private class Listener implements MessageListener
  {
    XAQueueSession mySession = null;
    QueueSender mySender = null;

    public Listener(XAQueueSession mySession, QueueSender mySender)
    {
      this.mySession = mySession;
      this.mySender = mySender;
    }

    public void onMessage(Message msg)
    {
      try
      {
        XAResource xares = mySession.getXAResource();
        Xid xid = new XidImpl(getClass().getName());
        xares.start(xid, XAResource.TMNOFLAGS);
        verifier.add(msg);
        mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        xares.end(xid, XAResource.TMSUCCESS);
        xares.prepare(xid);
        xares.commit(xid, false);
      } catch (Exception e)
      {
        exception = e;
        sem.notifySingleWaiter();
      }
      inc();
    }
  }
}
