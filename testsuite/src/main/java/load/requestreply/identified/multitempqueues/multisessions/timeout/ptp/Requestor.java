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

package load.requestreply.identified.multitempqueues.multisessions.timeout.ptp;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.*;

public class Requestor extends SimpleConnectedPTPTestCase
{
  int n = 0;

  public Requestor(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    n = Integer.parseInt(System.getProperty("load.requestreply.msgs", "1000"));
  }

  public void testRequest()
  {
    try
    {
      for (int i = 0; i < n; i++)
      {
        TextMessage msg = qs.createTextMessage();
        TemporaryQueue tempQueue = qs.createTemporaryQueue();
        QueueReceiver tempReceiver = qs.createReceiver(tempQueue);
        msg.setJMSReplyTo(tempQueue);
        msg.setText("Request: " + i);
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 500000);
        TextMessage reply = (TextMessage) tempReceiver.receive(200000);
        if (reply == null)
          fail("timeout occured on reply");
        tempReceiver.close();
        tempQueue.delete();
      }
      pause(3000);
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

