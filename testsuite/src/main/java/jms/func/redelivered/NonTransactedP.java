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

package jms.func.redelivered;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class NonTransactedP extends SimpleConnectedPTPTestCase
{
  public NonTransactedP(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE);
  }

  public void testNonTransactedP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setIntProperty("id", i);
        msg.setText("Msg: " + i);
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      int ids[] = new int[3];
      for (int i = 0; i < 3; i++)
      {
        msg = (TextMessage) receiver.receive();
        ids[i] = msg.getIntProperty("id");
      }
      qs.recover();

      for (int i = 0; i < 3; i++)
      {
        msg = (TextMessage) receiver.receive();
        int id = msg.getIntProperty("id");
        assertTrue("Does not receive right msg, expected: " + ids[i] + ", received: " + id, id == ids[i]);
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg not marked as redelivered", redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 2);
      }

      for (int i = 3; i < 10; i++)
      {
        msg = (TextMessage) receiver.receive();
        msg.acknowledge();
        boolean redelivered = msg.getJMSRedelivered();
        assertTrue("Msg marked as redelivered", !redelivered);
        int cnt = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("Invalid delivery count: " + cnt, cnt == 1);
      }
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

