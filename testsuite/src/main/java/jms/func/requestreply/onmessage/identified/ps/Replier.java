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

package jms.func.requestreply.onmessage.identified.ps;

import jms.base.SimpleConnectedPSTestCase;

import javax.jms.*;

public class Replier extends SimpleConnectedPSTestCase
{
  Object sem = new Object();
  int cnt = 0;

  public Replier(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
  }

  public void testReply()
  {
    try
    {
      subscriber.setMessageListener(new MessageListener()
      {
        public void onMessage(Message message)
        {
          try
          {
            TextMessage msg = (TextMessage) message;
            TopicPublisher replyPublisher = ts.createPublisher((TemporaryTopic) msg.getJMSReplyTo());
            msg.clearBody();
            msg.setText("Re: " + msg.getText());
            replyPublisher.publish(msg);
            replyPublisher.close();
            cnt++;
            if (cnt == 10000)
            {
              synchronized (sem)
              {
                sem.notify();
              }
            }
          } catch (Exception e1)
          {
            failFast("onMessage failed: " + e1);
          }
        }
      });
      try
      {
        synchronized (sem)
        {
          sem.wait();
        }
      } catch (Exception ignored)
      {
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

