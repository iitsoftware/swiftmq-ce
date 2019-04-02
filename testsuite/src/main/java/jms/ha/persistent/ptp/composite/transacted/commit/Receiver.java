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

package jms.ha.persistent.ptp.composite.transacted.commit;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.Message;
import javax.jms.QueueReceiver;
import javax.jms.Session;

public class Receiver extends SimpleConnectedPTPTestCase
{
  int nMsgs = 0;
  MsgNoVerifier verifier = null;
  String myQueueName = null;
  QueueReceiver myReceiver = null;

  public Receiver(String name, String myQueueName, int nMsgs)
  {
    super(name);
    this.myQueueName = myQueueName;
    this.nMsgs = nMsgs;
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.AUTO_ACKNOWLEDGE, false, false);
    myReceiver = qs.createReceiver(getQueue(myQueueName));
    verifier = new MsgNoVerifier(this, nMsgs, "no");
    verifier.setCheckSequence(true);
  }

  public void receive()
  {
    try
    {
      for (int i = 0; i < nMsgs; i++)
      {
        Message msg = myReceiver.receive();
        if (msg == null)
          throw new Exception("null message received!");
        verifier.add(msg);
        if ((i + 1) % 10 == 0)
          qs.commit();
      }
      Message msg = myReceiver.receive(2000);
      if (msg != null)
        throw new Exception("Received more messages than expected!");
      verifier.verify();
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    verifier = null;
    myReceiver.close();
    super.tearDown();
  }
}

