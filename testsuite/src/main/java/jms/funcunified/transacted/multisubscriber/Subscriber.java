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

package jms.funcunified.transacted.multisubscriber;

import jms.base.SimpleConnectedUnifiedPSTestCase;

import javax.jms.*;

public class Subscriber extends SimpleConnectedUnifiedPSTestCase
{
  Object sem = new Object();
  int cnt = 0;

  public Subscriber(String name)
  {
    super(name);
  }

  protected void setUp() throws Exception
  {
    setUp(true, Session.CLIENT_ACKNOWLEDGE);
  }

  public void testSubscribe()
  {
    try
    {
      consumer.setMessageListener(null);
      consumer.setMessageListener(new MessageListener()
      {
        public void onMessage(Message message)
        {
          synchronized (sem)
          {
            cnt++;
            TextMessage tm = (TextMessage) message;
            if (cnt == 20)
            {
              try
              {
                ts.commit();
              } catch (Exception jmse)
              {
                failFast(jmse.toString());
              }
              sem.notify();
            }
          }
        }
      });
      synchronized (sem)
      {
        if (cnt != 10)
        {
          try
          {
            sem.wait();
          } catch (Exception ignored)
          {
          }
        }
      }
      consumer.setMessageListener(null);
      TextMessage msg = (TextMessage) consumer.receive(2000);
      assertTrue("Received msg!=null", msg == null);
    } catch (Exception e)
    {
      failFast("test failed: " + e);
    }
  }
}

