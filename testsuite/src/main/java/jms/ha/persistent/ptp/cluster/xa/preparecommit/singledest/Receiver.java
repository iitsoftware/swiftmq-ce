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

package jms.ha.persistent.ptp.cluster.xa.preparecommit.singledest;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedXAPTPClusterTestCase;
import jms.base.XidImpl;

import javax.jms.Message;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Receiver extends SimpleConnectedXAPTPClusterTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));
  long initDelay = Long.parseLong(System.getProperty("jms.ha.cluster.receive.initdelay", "20000"));
  MsgNoVerifier verifier = null;

  public Receiver(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    pause(initDelay);
    super.setUp();
    verifier = new MsgNoVerifier(this, nMsgs, "no", true);
    verifier.setCheckSequence(false);
  }

  public void receive()
  {
    try
    {
      sender.close();
      Xid xid = new XidImpl(getClass().getName());
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int i = 0; i < nMsgs; i++)
      {
        Message msg = receiver.receive(120000);
        if (msg == null)
          break;
        verifier.add(msg);
        if ((i + 1) % 10 == 0)
        {
          xares.end(xid, XAResource.TMSUCCESS);
          xares.prepare(xid);
          xares.commit(xid, false);
          if (i + 1 < nMsgs)
          {
            xid = new XidImpl(getClass().getName());
            xares.start(xid, XAResource.TMNOFLAGS);
          }
        }
      }
      verifier.verify();
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    verifier = null;
    super.tearDown();
  }

}

