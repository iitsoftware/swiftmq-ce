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

package jms.func.msg;

import junit.framework.*;

public class Suite extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(new Messages("testMessageImpl"));
    suite.addTest(new Messages("testBytesMessageImpl"));
    suite.addTest(new Messages("testStreamMessageImpl"));
    suite.addTest(new Messages("testMapMessageImpl"));
    suite.addTest(new Messages("testObjectMessageImpl"));
    suite.addTest(new Messages("testTextMessageImpl"));
    suite.addTest(new Messages("testTextMessageImplLargeText"));
    return suite;
  }

  public String toString()
  {
    return "Message Properties/Values";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

