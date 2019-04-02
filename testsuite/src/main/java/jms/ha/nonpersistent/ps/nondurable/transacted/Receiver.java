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

package jms.ha.nonpersistent.ps.nondurable.transacted;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPSTestCase;

import javax.jms.Message;
import javax.jms.Session;

public class Receiver extends SimpleConnectedPSTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
  MsgNoVerifier verifier = null;

  public Receiver(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.AUTO_ACKNOWLEDGE, false, false, true);
    verifier = new MsgNoVerifier(this, nMsgs, "no");
    verifier.setCheckSequence(false);
    verifier.setMissingOk(true);
  }

  public void receive()
  {
    try
    {
      for (int i = 0; i < nMsgs; i++)
      {
        Message msg = subscriber.receive(120000);
        if (msg == null)
          break;
        verifier.add(msg);
        ts.commit();
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

