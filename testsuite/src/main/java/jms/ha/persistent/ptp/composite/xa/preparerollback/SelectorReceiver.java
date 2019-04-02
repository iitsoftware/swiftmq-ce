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

package jms.ha.persistent.ptp.composite.xa.preparerollback;

import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.MsgNoVerifier;
import jms.base.Checker;
import jms.base.XidImpl;

import javax.jms.QueueReceiver;
import javax.jms.Message;
import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;

public class SelectorReceiver extends SimpleConnectedXAPTPTestCase
{
  int nMsgs = 0;
  int maxMsgs = 0;
  MsgNoVerifier verifier = null;
  String myQueueName = null;
  QueueReceiver myReceiver = null;
  Checker checker = null;

  public SelectorReceiver(String name, String myQueueName, int maxMsgs, int nMsgs, Checker checker)
  {
    super(name);
    this.myQueueName = myQueueName;
    this.maxMsgs = maxMsgs;
    this.nMsgs = nMsgs;
    this.checker = checker;
  }

  protected void setUp() throws Exception
  {
    createSender = false;
    createReceiver = false;
    super.setUp();
    myReceiver = qs.getQueueSession().createReceiver(getQueue(myQueueName));
    verifier = new MsgNoVerifier(this, maxMsgs, "no");
    verifier.setCheckSequence(false);
    verifier.setMissingOk(true);
  }

  public void receive()
  {
    try
    {
      boolean rollback = false;
      int n = 0, m = 0;
      Xid xid = new XidImpl(getClass().getName());
      xares.start(xid, XAResource.TMNOFLAGS);
      while (n < nMsgs)
      {
        Message msg = myReceiver.receive(120000);
        if (msg == null)
          throw new Exception("null message received!");
        if (rollback)
        {
          m++;
          if (m == 10)
          {
            xares.end(xid, XAResource.TMSUCCESS);
            xares.prepare(xid);
            xares.rollback(xid);
            xid = new XidImpl(getClass().getName());
            xares.start(xid, XAResource.TMNOFLAGS);
            rollback = false;
            m = 0;
          }
          continue;
        }
        verifier.add(msg);
        n++;
        if (n % 10 == 0)
        {
          xares.end(xid, XAResource.TMSUCCESS);
          xares.prepare(xid);
          xares.commit(xid, false);
          if (n < nMsgs)
          {
            xid = new XidImpl(getClass().getName());
            xares.start(xid, XAResource.TMNOFLAGS);
          }
          rollback = true;
        }
      }
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
