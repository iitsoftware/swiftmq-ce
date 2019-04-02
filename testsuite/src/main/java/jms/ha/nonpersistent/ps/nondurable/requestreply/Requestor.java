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

package jms.ha.nonpersistent.ps.nondurable.requestreply;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPSTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;

public class Requestor extends SimpleConnectedPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  TopicSubscriber tempSubscriber = null;
  TemporaryTopic tempTopic = null;
  MsgNoVerifier verifier = null;

  public Requestor(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    subscriber.close();
    tempTopic = ts.createTemporaryTopic();
    tempSubscriber = ts.createSubscriber(tempTopic);
    verifier = new MsgNoVerifier(this, nMsgs, "no");
  }

  public void request()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < nMsgs; i++)
      {
        TextMessage reply = null;
        do
        {
          msg.setIntProperty("no", i);
          msg.setJMSReplyTo(tempTopic);
          msg.setText("Request: " + i);
          publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
          reply = (TextMessage) tempSubscriber.receive(60000);
        } while (reply == null);
      }
      msg.setIntProperty("no", nMsgs + 1);
      msg.setBooleanProperty("finished", true);
      publisher.publish(msg, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    tempSubscriber = null;
    tempTopic = null;
    verifier = null;
    super.tearDown();
  }
}

