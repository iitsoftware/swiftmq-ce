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

package jms.funcunified.browser;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;
import java.util.Enumeration;

public class Tester extends SimpleConnectedUnifiedPTPTestCase
{
  public Tester(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.CLIENT_ACKNOWLEDGE);
  }

  public void test()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 10; i++)
      {
        msg.setIntProperty("id", i);
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      qs.commit();


      QueueBrowser browser = qs.createBrowser(queue);
      int cnt = 0;
      Enumeration _enum = browser.getEnumeration();
      while (_enum.hasMoreElements())
      {
        TextMessage m = (TextMessage) _enum.nextElement();
        cnt++;
      }
      assertTrue("Invalid msg count in browser, received: " + cnt + ", expected 10", cnt == 10);

      for (int i = 0; i < 10; i++)
      {
        msg = (TextMessage) consumer.receive();
      }
      qs.commit();
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

