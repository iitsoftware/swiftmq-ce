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

package jms.xaunified.rollback.multidest.mixed;

import jms.base.SimpleConnectedUnifiedXAMixedTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.transaction.xa.*;

public class Tester extends SimpleConnectedUnifiedXAMixedTestCase
{
  public Tester(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(3,3);
  }

  public void test()
  {
    try
    {
      Xid xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      TextMessage msg = ts.createTextMessage();
      for (int j = 0; j < addProducer.length; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg.setText("Msg: " + i);
          addProducer[j].send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.prepare(xid);
      xares.rollback(xid);

      xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      msg = ts.createTextMessage();
      for (int j = 0; j < addProducer.length; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg.setText("Msg: " + i);
          addProducer[j].send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.prepare(xid);
      xares.commit(xid,false);

      xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int j = 0; j < addConsumer.length; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg = (TextMessage) addConsumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.prepare(xid);
      xares.rollback(xid);

      xid = new XidImpl();
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int j = 0; j < addConsumer.length; j++)
      {
        for (int i = 0; i < 5; i++)
        {
          msg = (TextMessage) addConsumer[j].receive(2000);
          assertTrue("Received msg==null", msg != null);
          boolean redelivered = msg.getJMSRedelivered();
          assertTrue("Msg not marked as redelivered", redelivered);
          int cnt = msg.getIntProperty("JMSXDeliveryCount");
          assertTrue("Invalid delivery count: " + cnt, cnt == 2);
        }
      }
      xares.end(xid, XAResource.TMSUCCESS);
      xares.prepare(xid);
      xares.commit(xid, false);
    } catch (Exception e)
    {
      e.printStackTrace();
      failFast("test failed: " + e);
    }
  }
}
