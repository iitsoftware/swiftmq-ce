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

package jms.ha.persistent.ptp.nontransacted.clientack.single.recover.receive;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.Message;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

public class Receiver extends SimpleConnectedPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  MsgNoVerifier verifier = null;
  QueueSession s1 = null;
  QueueSession s2 = null;
  QueueSession s3 = null;
  QueueReceiver receiver1 = null;
  QueueReceiver receiver2 = null;
  QueueReceiver receiver3 = null;

  public Receiver(String name)
  {
    super(name);
  }

  protected void beforeCreateSession() throws Exception
  {
    s1 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
    s2 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
    s3 = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
  }

  protected void afterCreateSession() throws Exception
  {
    s1.close();
    s2.close();
    s3.close();
  }

  protected void beforeCreateReceiver() throws Exception
  {
    receiver1 = qs.createReceiver(queue);
    receiver2 = qs.createReceiver(queue);
    receiver3 = qs.createReceiver(queue);
  }

  protected void afterCreateReceiver() throws Exception
  {
    receiver1.close();
    receiver2.close();
    receiver3.close();
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE, false, true);
    verifier = new MsgNoVerifier(this, nMsgs, "no");
  }

  public void receive()
  {
    try
    {
      boolean recover = false;
      int n = 0, m = 0;
      while (n < nMsgs)
      {
        Message msg = receiver.receive();
        if (msg == null)
          throw new Exception("null message received!");
        if (recover)
        {
          System.out.println("during recover, ignore: " + msg.getIntProperty("no"));
          m++;
          if (m == 10)
          {
            System.out.println("recover session!");
            qs.recover();
            recover = false;
            m = 0;
          }
          continue;
        }
        System.out.println("accepted: " + msg.getIntProperty("no"));
        verifier.add(msg);
        n++;
        if (n % 10 == 0)
        {
          System.out.println("ack message!");
          msg.acknowledge();
          recover = true;
        }
      }
      verifier.verify();
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    verifier = null;
    s1 = null;
    s2 = null;
    s3 = null;
    receiver1 = null;
    receiver2 = null;
    receiver3 = null;
    super.tearDown();
  }

}

