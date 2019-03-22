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

package jms.func.selector.load.preload;

import jms.func.selector.load.Sender;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

public class SenderSuite extends ActiveTestSuite
{

  public static Test suite()
  {
    TestSuite suite = new jms.func.selector.load.preload.SenderSuite();
    suite.addTest(new Sender("send", 0));
    suite.addTest(new Sender("send", 1));
    suite.addTest(new Sender("send", 2));
    suite.addTest(new Sender("send", 3));
    return suite;
  }

  public String toString()
  {
    return "preloadsender";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(jms.func.selector.load.preload.SenderSuite.suite());
  }
}
