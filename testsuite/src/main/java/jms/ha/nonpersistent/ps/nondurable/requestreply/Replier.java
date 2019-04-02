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

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;

public class Replier extends SimpleConnectedPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));

  public Replier(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
  }

  public void reply()
  {
    try
    {
      boolean finished = false;
      while (!finished)
      {
        TextMessage msg = (TextMessage) subscriber.receive();
        finished = msg.getBooleanProperty("finished");
        if (!finished)
        {
          try
          {
            TopicPublisher replyPublisher = ts.createPublisher((TemporaryTopic) msg.getJMSReplyTo());
            String s = msg.getText();
            msg.clearBody();
            msg.setText("Re: " + s);
            replyPublisher.publish(msg);
            replyPublisher.close();
          } catch (JMSException e)
          {
            // no problem (reconnected)
          }
        }
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

