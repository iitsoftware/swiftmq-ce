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

package amqp.v100.ptp.recover.receive.exactly_once;

import com.swiftmq.amqp.v100.client.QoS;
import amqp.v100.base.Util;
import amqp.v100.ptp.recover.receive.ReceiveTester;
import junit.framework.Test;
import junit.framework.TestSuite;

public class Suite extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(new ReceiveTester("test", QoS.EXACTLY_ONCE, Util.getQueueNamePrefix() + "1", false, false));
    return suite;
  }

  public String toString()
  {
    return "exctly_once";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

