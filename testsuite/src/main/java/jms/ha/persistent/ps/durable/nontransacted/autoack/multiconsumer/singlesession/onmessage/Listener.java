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

package jms.ha.persistent.ps.durable.nontransacted.autoack.multiconsumer.singlesession.onmessage;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPSTestCase;
import com.swiftmq.tools.concurrent.Semaphore;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

public class Listener extends SimpleConnectedPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  int n = 0;
  Exception exception = null;
  TopicSubscriber subscriber1 = null;
  TopicSubscriber subscriber2 = null;

  public Listener(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, true, false, true);
    subscriber1 = ts.createDurableSubscriber(getTopic("testtopic1"), "dur1");
    subscriber2 = ts.createDurableSubscriber(getTopic("testtopic2"), "dur2");
  }

  public void receive()
  {
    try
    {
      Semaphore sem1 = new Semaphore();
      subscriber.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem1));
      Semaphore sem2 = new Semaphore();
      subscriber1.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem2));
      Semaphore sem3 = new Semaphore();
      subscriber2.setMessageListener(new MyListener(new MsgNoVerifier(this, nMsgs, "no"), sem3));
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
    subscriber1.close();
    subscriber2.close();
    ts.unsubscribe("dur1");
    ts.unsubscribe("dur2");
    exception = null;
    subscriber1 = null;
    subscriber2 = null;
    super.tearDown();
  }

  private class MyListener implements MessageListener
  {
    MsgNoVerifier verifier = null;
    Semaphore sem = null;
    int n = 0;

    public MyListener(MsgNoVerifier verifier, Semaphore sem)
    {
      this.verifier = verifier;
      this.sem = sem;
    }

    public void onMessage(Message message)
    {
      try
      {
        verifier.add(message);
        n++;
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

