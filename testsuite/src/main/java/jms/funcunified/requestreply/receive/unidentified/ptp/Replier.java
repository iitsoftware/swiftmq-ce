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

package jms.funcunified.requestreply.receive.unidentified.ptp;

import jms.base.SimpleConnectedUnifiedPTPTestCase;

import javax.jms.*;

public class Replier extends SimpleConnectedUnifiedPTPTestCase
{
  MessageProducer replyProducer = null;

  public Replier(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE);
    replyProducer = qs.createProducer(null);
  }

  public void testReply()
  {
    try
    {
      for (int i = 0; i < 10000; i++)
      {
        TextMessage msg = (TextMessage) consumer.receive();
        msg.clearBody();
        msg.setText("Re: " + msg.getText());
        replyProducer.send((TemporaryQueue) msg.getJMSReplyTo(), msg);
      }
    } catch (Exception e)
    {
      e.printStackTrace();
      fail("test failed: " + e);
    }
  }
}

