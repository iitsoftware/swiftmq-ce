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

package jms.func.selector;

import junit.framework.Test;
import junit.framework.TestSuite;

public class Suite extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(new PTP("testQR1NP"));
    suite.addTest(new PTP("testQR2NP"));
    suite.addTest(new PTP("testQR3NP"));
    suite.addTest(new PTP("testQR4NP"));
    suite.addTest(new PTP("testQR5NP"));
    suite.addTest(new PTP("testQR1P"));
    suite.addTest(new PTP("testQR2P"));
    suite.addTest(new PTP("testQR3P"));
    suite.addTest(new PTP("testQR4P"));
    suite.addTest(new PTP("testQR5P"));
    suite.addTest(new PTP("testQR6P"));
    suite.addTest(new PTP("testQR8P"));
    suite.addTest(new PTPML("testQR1NP"));
    suite.addTest(new PTPML("testQR2NP"));
    suite.addTest(new PTPML("testQR3NP"));
    suite.addTest(new PTPML("testQR4NP"));
    suite.addTest(new PTPML("testQR1P"));
    suite.addTest(new PTPML("testQR2P"));
    suite.addTest(new PTPML("testQR3P"));
    suite.addTest(new PTPML("testQR4P"));
    suite.addTest(new PS("testTS1NP"));
    suite.addTest(new PS("testTS2NP"));
    suite.addTest(new PS("testTS3NP"));
    suite.addTest(new PS("testTS4NP"));
    suite.addTest(new PS("testTS1P"));
    suite.addTest(new PS("testTS2P"));
    suite.addTest(new PS("testTS3P"));
    suite.addTest(new PS("testTS4P"));
    suite.addTest(new PS("testTS5P"));
    suite.addTest(new PSML("testTS1NP"));
    suite.addTest(new PSML("testTS2NP"));
    suite.addTest(new PSML("testTS3NP"));
    suite.addTest(new PSML("testTS4NP"));
    suite.addTest(new PSML("testTS1P"));
    suite.addTest(new PSML("testTS2P"));
    suite.addTest(new PSML("testTS3P"));
    suite.addTest(new PSML("testTS4P"));
    suite.addTest(jms.func.selector.load.Suite.suite());
    return suite;
  }

  public String toString()
  {
    return "Message Selectors";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

