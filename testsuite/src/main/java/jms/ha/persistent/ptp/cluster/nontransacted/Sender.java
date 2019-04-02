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

public class Sender extends SimpleConnectedPTPClusterTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));
  QueueSession s1 = null;
  QueueSession s2 = null;
  QueueSession s3 = null;
  QueueSender sender1 = null;
  QueueSender sender2 = null;
  QueueSender sender3 = null;

  public Sender(String name)
  {
    super(name);
  }

  protected void beforeCreateSession() throws Exception
  {
    s1 = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    s2 = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    s3 = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  protected void afterCreateSession() throws Exception
  {
    s1.close();
    s2.close();
    s3.close();
  }

  protected void beforeCreateSender() throws Exception
  {
    sender1 = qs.createSender(queue);
    sender2 = qs.createSender(queue);
    sender3 = qs.createSender(queue);
  }

  protected void afterCreateSender() throws Exception
  {
    sender1.close();
    sender2.close();
    sender3.close();
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, true, false);
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
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    s1 = null;
    s2 = null;
    s3 = null;
    sender1 = null;
    sender2 = null;
    sender3 = null;
    super.tearDown();
  }
}

