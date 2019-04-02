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

package jms.xa.specials.concurrentcommit;

import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.*;
import javax.transaction.xa.*;

public class Tester extends SimpleConnectedXAPTPTestCase
{
  public Tester(String name)
  {
    super(name);
  }

  public void testP()
  {
    try
    {
      Xid xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      TextMessage msg = qs.createTextMessage();
      for (int i = 0; i < 2; i++)
      {
        msg.setText("Msg1: " + i);
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.end(xid1, XAResource.TMSUCCESS);
      Xid xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      for (int i = 0; i < 3; i++)
      {
        msg.setText("Msg2: " + i);
        sender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
      }
      xares.prepare(xid1);
      xares.commit(xid1, false);
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);

      xid1 = new XidImpl();
      xares.start(xid1, XAResource.TMNOFLAGS);
      for (int i = 0; i < 2; i++)
      {
        msg = (TextMessage) receiver.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      xares.end(xid1, XAResource.TMSUCCESS);
      xid2 = new XidImpl();
      xares.start(xid2, XAResource.TMNOFLAGS);
      xares.prepare(xid1);
      xares.commit(xid1, false);
      for (int i = 0; i < 3; i++)
      {
        msg = (TextMessage) receiver.receive(2000);
        assertTrue("Received msg==null", msg != null);
      }
      xares.end(xid2, XAResource.TMSUCCESS);
      xares.prepare(xid2);
      xares.commit(xid2, false);
    } catch (Exception e)
    {
      e.printStackTrace();
      failFast("test failed: " + e);
    }
  }
}
