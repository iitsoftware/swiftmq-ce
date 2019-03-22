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

package jms.xaunified.suspendresume.multitxlocal.singledest.ps;

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
      Xid xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 2; i++)
      {
        msg.setText("Msg1: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid1, XAResource.TMSUSPEND);
      Xid xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg2: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid2, XAResource.TMSUSPEND);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg3: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.getSession().commit();
      xares.start(xid1, XAResource.TMRESUME);
      xares.end(xid1, XAResource.TMSUCCESS);
      xares.prepare(xid1);
      xares.commit(xid1, false);
      xares.start(xid2, XAResource.TMRESUME);
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);

      xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 3; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      xares.end(xid1, XAResource.TMSUSPEND);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 3; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      ts.getSession().commit();
      xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 2; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      xares.end(xid2, XAResource.TMSUSPEND);
      xares.start(xid1, XAResource.TMRESUME);
      xares.end(xid1, XAResource.TMSUCCESS);
      xares.prepare(xid1);
      xares.commit(xid1, false);
      xares.start(xid2, XAResource.TMRESUME);
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);
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
      Xid xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 2; i++)
      {
        msg.setText("Msg1: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid1, XAResource.TMSUSPEND);
      Xid xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg2: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid2, XAResource.TMSUSPEND);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg3: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.getSession().commit();
      xares.start(xid1, XAResource.TMRESUME);
      xares.end(xid1, XAResource.TMSUCCESS);
      xares.prepare(xid1);
      xares.commit(xid1, false);
      xares.start(xid2, XAResource.TMRESUME);
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);

      xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 3; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      xares.end(xid1, XAResource.TMSUSPEND);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 3; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      ts.getSession().commit();
      xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      for (int j = 0; j < 3; j++)
      {
        for (int i = 0; i < 2; i++)
        {
          msg = (TextMessage) consumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      xares.end(xid2, XAResource.TMSUSPEND);
      xares.start(xid1, XAResource.TMRESUME);
      xares.end(xid1, XAResource.TMSUCCESS);
      xares.prepare(xid1);
      xares.commit(xid1, false);
      xares.start(xid2, XAResource.TMRESUME);
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);
    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }
}
