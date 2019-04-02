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

package jms.ha.persistent.ptp.cluster.xa.cc.singlesession;

import jms.base.MsgNoVerifier;
import jms.base.ServerSessionImpl;
import jms.base.ServerSessionPoolImpl;
import jms.base.SimpleConnectedXAPTPCCClusterTestCase;
import jms.base.XidImpl;
import com.swiftmq.tools.concurrent.Semaphore;

import javax.jms.ConnectionConsumer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.XAQueueSession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Consumer extends SimpleConnectedXAPTPCCClusterTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));
  long initDelay = Long.parseLong(System.getProperty("jms.ha.cluster.receive.initdelay", "20000"));
  MsgNoVerifier verifier = null;
  ConnectionConsumer cc = null;
  ServerSessionPoolImpl pool = null;
  Semaphore sem = null;
  Exception exception = null;

  public Consumer(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    pause(initDelay);
    super.setUp();
    sender.close();
    receiver.close();
    qs.close();
    pool = new ServerSessionPoolImpl();
    for (int i = 0; i < 1; i++)
    {
      XAQueueSession session = qc.createXAQueueSession();
      session.setMessageListener(new Listener(session));
      pool.addServerSession(new ServerSessionImpl(pool, session));
    }
    cc = qc.createConnectionConsumer(queue, null, pool, 5);
    verifier = new MsgNoVerifier(this, nMsgs, "no", true);
    verifier.setCheckSequence(false);
  }

  public void consume()
  {
    sem = new Semaphore();
    sem.waitHere();
    if (exception != null)
      failFast("failed: " + exception);
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
    int n = 0;

    public Listener(XAQueueSession mySession)
    {
      this.mySession = mySession;
    }

    public void onMessage(Message msg)
    {
      n++;
      try
      {
        XAResource xares = mySession.getXAResource();
        Xid xid = new XidImpl(getClass().getName());
        xares.start(xid, XAResource.TMNOFLAGS);
        verifier.add(msg);
        xares.end(xid, XAResource.TMSUCCESS);
        xares.prepare(xid);
        xares.commit(xid, false);
      } catch (Exception e)
      {
        exception = e;
        sem.notifySingleWaiter();
      }
      if (n == nMsgs)
      {
        try
        {
          verifier.verify();
        } catch (Exception e)
        {
          exception = e;
        }
        sem.notifySingleWaiter();
      }
    }
  }
}
