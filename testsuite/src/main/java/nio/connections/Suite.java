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

package nio.connections;

import junit.extensions.ActiveTestSuite;
import junit.framework.*;

public class Suite extends ActiveTestSuite
{
  public static Test suite()
  {
    int n = 50;
    TestSuite suite = new Suite();
    for (int i = 0; i < n; i++)
    {
      suite.addTest(new Tester("test"));
    }
    return suite;
  }

  public String toString()
  {
    return "connections";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

