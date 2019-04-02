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

package jms.ha.persistent.ps.durable.xa.onephase;

import jms.base.SimpleConnectedXAPSTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Sender extends SimpleConnectedXAPSTestCase
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
    super.setUp(false, false);
    myPublisher = ts.getTopicSession().createPublisher(getTopic(topicName));
    pause(20000);
  }

  public void send()
  {
    try
    {
      TextMessage msg = ts.createTextMessage();
      for (int i = 0; i < nMsgs; i++)
      {
        Xid xid = new XidImpl(getClass().getName());
        xares.start(xid, XAResource.TMNOFLAGS);
        msg.setIntProperty("no", i);
        msg.setText("Msg: " + i);
        myPublisher.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        xares.end(xid, XAResource.TMSUCCESS);
        xares.commit(xid, true);
      }

    } catch (Exception e)
    {
      e.printStackTrace();
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

