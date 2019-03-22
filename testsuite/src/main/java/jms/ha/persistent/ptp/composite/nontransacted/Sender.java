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

package jms.ha.persistent.ptp.composite.nontransacted;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueSender;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Sender extends SimpleConnectedPTPTestCase
{
  String myQueueName = System.getProperty("jndi.composite.queue");
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.composite.nmsgs", "20000"));
  QueueSender mySender = null;

  public Sender(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, false, false);
    mySender = qs.createSender(getQueue(myQueueName));
  }

  public void send()
  {
    try
    {
      for (int i = 0; i < nMsgs; i++)
      {
        TextMessage msg = qs.createTextMessage();
        msg.setIntProperty("no", i);
        if (i % 100 == 0)
          msg.setStringProperty("Prop", "X");
        msg.setText("Msg: " + i);
        mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    mySender.close();
    super.tearDown();
  }
}

