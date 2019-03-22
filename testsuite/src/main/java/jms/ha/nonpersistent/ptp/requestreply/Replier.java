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

package jms.ha.nonpersistent.ptp.requestreply;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.JMSException;
import javax.jms.QueueSender;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

public class Replier extends SimpleConnectedPTPTestCase
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
        TextMessage msg = (TextMessage) receiver.receive();
        finished = msg.getBooleanProperty("finished");
        if (!finished)
        {
          try
          {
            QueueSender replySender = qs.createSender((TemporaryQueue) msg.getJMSReplyTo());
            String s = msg.getText();
            msg.clearBody();
            msg.setText("Re: " + s);
            replySender.send(msg);
            replySender.close();
          } catch (JMSException e)
          {
            // no problem (reconnected)
          }
        }
      }
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

