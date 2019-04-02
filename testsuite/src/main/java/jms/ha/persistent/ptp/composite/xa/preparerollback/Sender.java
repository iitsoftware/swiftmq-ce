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
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueSender;
import javax.jms.TextMessage;
import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;

public class Sender extends SimpleConnectedXAPTPTestCase
{
  String myQueueName = System.getProperty("jndi.composite.queue");
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.composite.nmsgs", "20000"));
  QueueSender mySender = null;

  public Sender(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    mySender = qs.getQueueSession().createSender(getQueue(myQueueName));
  }

  public void send()
  {
    try
    {
      boolean rollback = false;
      int n = 0;
      Xid xid = new XidImpl(getClass().getName());
      xares.start(xid, XAResource.TMNOFLAGS);
      while (n < nMsgs)
      {
        TextMessage msg = qs.createTextMessage();
        msg.setIntProperty("no", n);
        if (n % 100 == 0)
          msg.setStringProperty("Prop", "X");
        msg.setText("Msg: " + n);
        mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        if ((n + 1) % 10 == 0)
        {
          xares.end(xid, XAResource.TMSUCCESS);
          xares.prepare(xid);
          if (rollback)
          {
            rollback = false;
            n -= 10;
            xares.rollback(xid);
          } else
          {
            xares.commit(xid, false);
            rollback = true;
          }
          if (n + 1 < nMsgs)
          {
            xid = new XidImpl(getClass().getName());
            xares.start(xid, XAResource.TMNOFLAGS);
          }
        }
        n++;
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    mySender.close();
    super.tearDown();
  }
}

