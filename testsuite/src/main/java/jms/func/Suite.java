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

package jms.func;

import junit.framework.*;

public class Suite extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(jms.func.msg.Suite.suite());
    suite.addTest(jms.func.prio.Suite.suite());
    suite.addTest(jms.func.nontransacted.Suite.suite());
    suite.addTest(jms.func.transacted.Suite.suite());
    suite.addTest(jms.func.redelivered.Suite.suite());
    suite.addTest(jms.func.selector.Suite.suite());
    suite.addTest(jms.func.requestreply.Suite.suite());
    suite.addTest(jms.func.browser.Suite.suite());
    return suite;
  }

  public String toString()
  {
    return "Functionality";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

