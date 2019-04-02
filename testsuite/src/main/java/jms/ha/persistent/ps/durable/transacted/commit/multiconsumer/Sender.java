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

package jms.ha.persistent.ps.durable.transacted.commit.multiconsumer;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;

public class Sender extends SimpleConnectedPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  String topicName = null;
  TopicPublisher myPublisher = null;

  public Sender(String name, String topicName)
  {
    super(name);
    this.topicName = topicName;
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, true, false, false);
    pause(20000);
    myPublisher = ts.createPublisher(getTopic(topicName));
  }

  public void send()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < nMsgs; i++)
      {
        msg.setIntProperty("no", i);
        msg.setText("Msg: " + i);
        myPublisher.publish(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }

    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    topicName = null;
    myPublisher = null;
    super.tearDown();
  }
}

