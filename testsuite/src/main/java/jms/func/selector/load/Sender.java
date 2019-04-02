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

package jms.func.selector.load;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Sender extends SimpleConnectedPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.func.selector.load.nmsgs", "25000"));
  int partition = -1;

  public Sender(String name, int partition)
  {
    super(name);
    this.partition = partition;
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
        msg.setIntProperty("partition", partition);
        msg.setIntProperty("no", i);
        msg.setText("Partition: " + partition + " Msg: " + i);
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

}
