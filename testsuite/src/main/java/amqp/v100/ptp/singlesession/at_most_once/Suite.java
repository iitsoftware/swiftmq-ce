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

package amqp.v100.ptp.singlesession.at_most_once;

import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.QoS;
import com.swiftmq.amqp.v100.client.Session;
import amqp.v100.base.Util;
import amqp.v100.ptp.singlesession.Receiver;
import amqp.v100.ptp.singlesession.Sender;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.concurrent.CountDownLatch;

public class Suite extends ActiveTestSuite
{
  public static Test suite()
  {
    int nPairs = Integer.parseInt(System.getProperty("npairs", "10"));
    TestSuite suite = new Suite();
    try
    {
      Connection connection = Util.createConnection();
      Session session = Util.createSession(connection);
      CountDownLatch countDownLatch = new CountDownLatch(nPairs);
      for (int i = 0; i < nPairs; i++)
      {
        suite.addTest(new Receiver("receive", QoS.AT_MOST_ONCE, Util.getQueueNamePrefix() + (i + 1), connection, session, countDownLatch));
        suite.addTest(new Sender("send", QoS.AT_MOST_ONCE, Util.getQueueNamePrefix() + (i + 1), connection, session));
      }
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    return suite;
  }

  public String toString()
  {
    return "at_most_once";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

