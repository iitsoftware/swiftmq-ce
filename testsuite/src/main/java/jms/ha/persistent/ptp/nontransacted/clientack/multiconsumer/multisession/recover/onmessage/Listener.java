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

package jms.ha.persistent.ptp.nontransacted.clientack.multiconsumer.multisession.recover.onmessage;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;
import com.swiftmq.tools.concurrent.Semaphore;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

public class Listener extends SimpleConnectedPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  Exception exception = null;
  QueueSession session1 = null;
  QueueSession session2 = null;
  QueueReceiver receiver1 = null;
  QueueReceiver receiver2 = null;

  public Listener(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE, false, true);
    session1 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
    session2 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
    receiver1 = session1.createReceiver(getQueue("testqueue1@router"));
    receiver2 = session2.createReceiver(getQueue("testqueue2@router"));
  }

  public void receive()
  {
    try
    {
      Semaphore sem1 = new Semaphore();
      receiver.setMessageListener(new MyListener(qs, new MsgNoVerifier(this, nMsgs, "no"), sem1));
      Semaphore sem2 = new Semaphore();
      receiver1.setMessageListener(new MyListener(session1, new MsgNoVerifier(this, nMsgs, "no"), sem2));
      Semaphore sem3 = new Semaphore();
      receiver2.setMessageListener(new MyListener(session2, new MsgNoVerifier(this, nMsgs, "no"), sem3));
      sem1.waitHere();
      sem2.waitHere();
      sem3.waitHere();
      if (exception != null)
        throw exception;
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    exception = null;
    session1 = null;
    session2 = null;
    receiver1 = null;
    receiver2 = null;
    super.tearDown();
  }

  private class MyListener implements MessageListener
  {
    QueueSession mySession = null;
    MsgNoVerifier verifier = null;
    Semaphore sem = null;
    int n = 0, m = 0;
    boolean recover = false;

    public MyListener(QueueSession mySession, MsgNoVerifier verifier, Semaphore sem)
    {
      this.mySession = mySession;
      this.verifier = verifier;
      this.sem = sem;
    }

    public void onMessage(Message message)
    {
      try
      {
        if (recover)
        {
          System.out.println("during recover, ignore: " + message.getIntProperty("no"));
          m++;
          if (m == 10)
          {
            System.out.println("recover session!");
            mySession.recover();
            recover = false;
            m = 0;
          }
          return;
        }
        System.out.println("ok: " + message.getIntProperty("no"));
        verifier.add(message);
        n++;
        if (n % 10 == 0)
        {
          System.out.println("ack message!");
          message.acknowledge();
          recover = true;
        }
        if (n == nMsgs)
          sem.notifySingleWaiter();
      } catch (Exception e)
      {
        exception = e;
        sem.notifySingleWaiter();
      }
    }
  }
}

