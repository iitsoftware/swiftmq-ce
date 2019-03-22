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

package jms.xa.specials.differentsessions.txtimeout;

import jms.base.MultisessionConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.transaction.xa.*;

public class Tester extends MultisessionConnectedXAPTPTestCase
{
  public Tester(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp(4);
  }

  public void testP()
  {
    try
    {
      XAResource xares1 = sessions[0].getXAResource();
      XAResource xares2 = sessions[1].getXAResource();
      XAResource xares3 = sessions[2].getXAResource();
      XAResource xares4 = sessions[3].getXAResource();

      int txto = xares1.getTransactionTimeout();
      assertTrue("Invalid tx timeout, 300 expected but was "+txto,txto == 300);

      txto = 600;
      assertTrue("xares2.setTransactionTimeout(txto) returns false",xares2.setTransactionTimeout(txto));
      int txto2 = xares1.getTransactionTimeout();
      assertTrue("Invalid tx timeout, "+txto+" expected but was "+txto2,txto == txto2);
      txto2 = xares2.getTransactionTimeout();
      assertTrue("Invalid tx timeout, "+txto+" expected but was "+txto2,txto == txto2);
      txto2 = xares3.getTransactionTimeout();
      assertTrue("Invalid tx timeout, "+txto+" expected but was "+txto2,txto == txto2);
      txto2 = xares4.getTransactionTimeout();
      assertTrue("Invalid tx timeout, "+txto+" expected but was "+txto2,txto == txto2);

      assertTrue("xares2.setTransactionTimeout(txto) returns false",xares2.setTransactionTimeout(0));
      txto = xares4.getTransactionTimeout();
      assertTrue("Invalid tx timeout, 300 expected but was "+txto,txto == 300);

      xares2.setTransactionTimeout(10);
      Xid xid = new XidImpl();
      QueueSender sender1 = sessions[0].getQueueSession().createSender(queue);
      xares1.start(xid, XAResource.TMNOFLAGS);
      TextMessage msg = sessions[0].createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg1: " + i);
        sender1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      pause(20000);
      try
      {
        xares1.end(xid, XAResource.TMSUCCESS);
        throw new Exception("Tx not rolled back after tx timeout!");
      } catch (XAException e)
      {
        //ok
      }
      xares2.setTransactionTimeout(10);
    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }
}
