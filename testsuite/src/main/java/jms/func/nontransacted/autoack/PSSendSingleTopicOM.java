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

package jms.func.nontransacted.autoack;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class PSSendSingleTopicOM extends SimpleConnectedPSTestCase
{
  TopicSession ts2 = null;
  TopicSubscriber subscriber2 = null;
  Object sem = new Object();
  int cnt = 0;

  public PSSendSingleTopicOM(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    subscriber.close();
    ts2 = tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    subscriber2 = ts2.createSubscriber(topic);
  }

  public void testPSSendSingleTopicOMNP()
  {
    try
    {
      subscriber2.setMessageListener(null);
      subscriber2.setMessageListener(new MessageListener()
      {
        public void onMessage(Message message)
        {
          synchronized (sem)
          {
            cnt++;
            TextMessage tm = (TextMessage) message;
            if (cnt == 10)
            {
              sem.notify();
            }
          }
        }
      });

      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      synchronized (sem)
      {
        if (cnt != 10)
        {
          try
          {
            sem.wait();
          } catch (Exception ignored)
          {
          }
        }
      }
      subscriber2.setMessageListener(null);
      msg = (TextMessage) subscriber2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  public void testPSSendSingleTopicOMP()
  {
    try
    {
      subscriber2.setMessageListener(null);
      subscriber2.setMessageListener(new MessageListener()
      {
        public void onMessage(Message message)
        {
          synchronized (sem)
          {
            cnt++;
            TextMessage tm = (TextMessage) message;
            if (cnt == 10)
            {
              sem.notify();
            }
          }
        }
      });

      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setText("Msg: " + i);
        publisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

      synchronized (sem)
      {
        if (cnt != 10)
        {
          try
          {
            sem.wait();
          } catch (Exception ignored)
          {
          }
        }
      }
      subscriber2.setMessageListener(null);
      msg = (TextMessage) subscriber2.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

