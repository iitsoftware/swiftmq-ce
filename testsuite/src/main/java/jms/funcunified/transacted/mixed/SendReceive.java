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

package jms.funcunified.transacted.mixed;

import jms.base.SimpleConnectedUnifiedMixedTestCase;

import javax.jms.*;

public class SendReceive extends SimpleConnectedUnifiedMixedTestCase
{

  public SendReceive(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.CLIENT_ACKNOWLEDGE, true);
  }

  public void test()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        producerQueue.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        producerTopic.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      ts.commit();
      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumerQueue.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumerTopic.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      ts.rollback();

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumerQueue.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumerTopic.receive(2000);
        assertTrue("Received msg==null", msg != null);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }
      ts.commit();
      msg = (TextMessage) consumerQueue.receive(2000);
      assertTrue("Received msg!=null", msg == null);
      msg = (TextMessage) consumerTopic.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
}

