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

package jms.func.nontransacted.clientack;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class PSSendSingleTopic extends SimpleConnectedPSTestCase
{
  public PSSendSingleTopic(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE);
  }

  public void testPSSendSingleTopicNP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) subscriber.receive(2000);
        msg.acknowledge();
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) subscriber.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSSendSingleTopicP()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }


      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) subscriber.receive(2000);
        msg.acknowledge();
        assertTrue("Received msg==null", msg != null);
      }

      msg = (TextMessage) subscriber.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

