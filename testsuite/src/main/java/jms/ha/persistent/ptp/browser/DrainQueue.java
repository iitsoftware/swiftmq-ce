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

package jms.ha.persistent.ptp.browser;

import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.Message;
import javax.jms.Session;

public class DrainQueue extends SimpleConnectedPTPTestCase
{
  int nMsgs = Integer.parseInt(System.getProperty("jms.ha.browser.nmsgs", "50000"));

  public DrainQueue(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(false, Session.AUTO_ACKNOWLEDGE, false, true);
  }

  public void drain()
  {
    try
    {
      for (int i = 0; i < nMsgs; i++)
      {
        Message msg = receiver.receive();
        if (msg == null)
          throw new Exception("null message received!");
      }
    } catch (Exception e)
    {
      fail("test failed: " + e);
    }
  }
}

