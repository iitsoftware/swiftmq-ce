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

package jms.xa.specials.differentsessions.noendrollback;

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
    super.setUp(5);
  }

  public void testP()
  {
    try
    {
      Xid xid = new XidImpl();
      XAResource xares1 = sessions[0].getXAResource();
      QueueSender sender1 = sessions[0].getQueueSession().createSender(queue);
      xares1.start(xid, XAResource.TMNOFLAGS);
      TextMessage msg = sessions[0].createTextMessage();
      for (int i = 0; i < 2; i++)
      {
        msg.setText("Msg1: " + i);
        sender1.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      XAResource xares2 = sessions[1].getXAResource();
      QueueSender sender2 = sessions[1].getQueueSession().createSender(queue);
      xares2.start(xid, XAResource.TMJOIN);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg2: " + i);
        sender2.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares2.end(xid, XAResource.TMSUCCESS);

      XAResource xares3 = sessions[2].getXAResource();
      QueueSender sender3 = sessions[2].getQueueSession().createSender(queue);
      xares3.start(xid, XAResource.TMJOIN);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg3: " + i);
        sender3.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      xares1.rollback(xid);

      System.out.println("1==2?"+xares1.isSameRM(xares2)+" 1==3?"+xares1.isSameRM(xares3)+" 2==3?"+xares2.isSameRM(xares3));
      try
      {
        xares1.end(xid, XAResource.TMSUCCESS);
      } catch (XAException e)
      {
        assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
      }
      try
      {
        xares3.end(xid, XAResource.TMSUCCESS);
      } catch (XAException e)
      {
        assertTrue("e.errorCode != XAException.XA_RBROLLBACK", e.errorCode == XAException.XA_RBROLLBACK);
      }
      try
      {
        XAResource xares4 = sessions[3].getXAResource();
        xares4.start(xid, XAResource.TMJOIN);
      } catch (XAException e)
      {
        assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
      }

    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }
}
