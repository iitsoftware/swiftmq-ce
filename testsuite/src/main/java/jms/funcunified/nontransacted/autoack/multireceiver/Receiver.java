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

package jms.funcunified.nontransacted.autoack.multireceiver;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class Receiver extends SimpleConnectedUnifiedPTPTestCase
{
  public Receiver(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
  }

  public void testReceive()
  {
    try
    {
      TextMessage msg = null;
      int cnt = 0;
      do
      {
        msg = (TextMessage) consumer.receive(10000);
        if (msg != null)
          cnt++;
      } while (msg != null);

      System.out.println("got " + cnt + " messages");
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

