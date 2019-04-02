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

package jms.ha.persistent.ptp.browser;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueSender;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Sender extends SimpleConnectedPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.browser.nmsgs", "50000"));
  String queueName = null;
  QueueSender mySender = null;

  public Sender(String name, String queueName)
  {
    super(name);
    this.queueName = queueName;
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, false, false);
    mySender = qs.createSender(getQueue(queueName));
  }

  public void send()
  {
    try
    {
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < nMsgs; i++)
      {
        msg.setIntProperty("no", i);
        msg.setText("Msg: " + i);
        mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    queueName = null;
    mySender = null;
    super.tearDown();
  }
}

