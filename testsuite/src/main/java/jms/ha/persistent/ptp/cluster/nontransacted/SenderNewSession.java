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

package jms.ha.persistent.ptp.cluster.nontransacted;

import jms.base.SimpleConnectedPTPClusterTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

public class SenderNewSession extends SimpleConnectedPTPClusterTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));

  public SenderNewSession(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, true, false);
    qs.close();
  }

  public void send()
  {
    try
    {
      for (int i = 0; i < nMsgs; i++)
      {
        QueueSession mySession = qc.createQueueSession(false, 0);
        QueueSender mySender = mySession.createSender(queue);
        TextMessage msg = mySession.createTextMessage();
        msg.setIntProperty("no", i);
        msg.setText("Msg: " + i);
        mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        mySender.close();
        mySession.close();
      }

    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
}
