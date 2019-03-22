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

package jms.xaunified.endfailprepare.singledest.ps;

import jms.base.SimpleConnectedUnifiedXAPS1MTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.transaction.xa.*;

public class Tester extends SimpleConnectedUnifiedXAPS1MTestCase
{
  public Tester(String name)
  {
    super(name);
  }

  public void testNP()
  {
    try
    {
      Xid xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 5; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid, XAResource.TMFAIL);
      try
      {
        xares.prepare(xid);
        fail("doesn't throws XAException!");
      } catch (XAException e)
      {
        xares.rollback(xid);
      }

      xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg!=null", msg == null);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.commit(xid, true);
    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }

  public void testP()
  {
    try
    {
      Xid xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 5; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid, XAResource.TMFAIL);
      try
      {
        xares.prepare(xid);
        fail("doesn't throws XAException!");
      } catch (XAException e)
      {
        xares.rollback(xid);
      }

      xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg!=null", msg == null);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.commit(xid, true);
    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }
}
