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

package jms.ha.persistent.ps.durable.nontransacted.autoack.multiproducer;

import com.swiftmq.tools.tracking.MessageTracker;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class Suite extends ActiveTestSuite
{
  public void run(TestResult testResult)
  {
    MessageTracker.getInstance().setPrefix(getClass().getPackage().getName());
    super.run(testResult);
  }

  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(new Listener("receive"));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    suite.addTest(new Sender("send", "testtopic", 50));
    return suite;
  }

  public String toString()
  {
    return "multiproducer";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}