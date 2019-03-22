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

package jms.ha.persistent.ps.durable.xa.preparecommit.singledest;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedXAPSTestCase;
import jms.base.XidImpl;

import javax.jms.Message;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Receiver extends SimpleConnectedXAPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  MsgNoVerifier verifier = null;

  public Receiver(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    super.setUp(true, false);
    verifier = new MsgNoVerifier(this, nMsgs, "no");
//    verifier.setCheckSequence(false);
  }

  public void receive()
  {
    try
    {
      Xid xid = new XidImpl(getClass().getName());
      xares.start(xid, XAResource.TMNOFLAGS);
      for (int i = 0; i < nMsgs; i++)
      {
        Message msg = subscriber.receive(360000);
        if (msg == null)
          break;
        verifier.add(msg);
        if ((i + 1) % 10 == 0)
        {
          xares.end(xid, XAResource.TMSUCCESS);
          xares.prepare(xid);
          xares.commit(xid, false);
          xid = new XidImpl(getClass().getName());
          xares.start(xid, XAResource.TMNOFLAGS);
        }
      }
      verifier.verify();
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }

  protected void tearDown() throws Exception
  {
    verifier = null;
    super.tearDown();
  }
}

