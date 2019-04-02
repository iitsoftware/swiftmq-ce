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

package jms.funcunified.nontransacted.clientack.multireceiver;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class Sender extends SimpleConnectedUnifiedPTPTestCase
{
  public Sender(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.CLIENT_ACKNOWLEDGE);
  }

  public void testSendNP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 100; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  public void testSendP()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 100; i++)
      {
        msg.setText("Msg: " + i);
        producer.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

