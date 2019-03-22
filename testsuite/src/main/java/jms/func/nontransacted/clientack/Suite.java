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

package jms.func.nontransacted.clientack;

import junit.framework.*;

public class Suite extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new Suite();
    suite.addTest(new PTPSendSingleQueue("testPTPSendSingleQueueNP"));
    suite.addTest(new PTPSendSingleQueue("testPTPSendSingleQueueP"));
    suite.addTest(new PTPSendSingleQueueOM("testPTPSendSingleQueueOMNP"));
    suite.addTest(new PTPSendSingleQueueOM("testPTPSendSingleQueueOMP"));
    suite.addTest(new PTPMultipleQueues("testPTPUnidentifiedNP"));
    suite.addTest(new PTPMultipleQueues("testPTPIdentifiedNP"));
    suite.addTest(new PTPMultipleQueues("testPTPUnidentifiedP"));
    suite.addTest(new PTPMultipleQueues("testPTPIdentifiedP"));
    suite.addTest(new PTPMultipleQueues("testPTPSendReceiveNP"));
    suite.addTest(new PTPMultipleQueues("testPTPSendReceiveP"));
    suite.addTest(new PSSendSingleTopic("testPSSendSingleTopicNP"));
    suite.addTest(new PSSendSingleTopic("testPSSendSingleTopicP"));
    suite.addTest(new PSSendSingleTopicOM("testPSSendSingleTopicOMNP"));
    suite.addTest(new PSSendSingleTopicOM("testPSSendSingleTopicOMP"));
    suite.addTest(new PSMultipleTopics("testPSUnidentifiedNP"));
    suite.addTest(new PSMultipleTopics("testPSIdentifiedNP"));
    suite.addTest(new PSMultipleTopics("testPSUnidentifiedP"));
    suite.addTest(new PSMultipleTopics("testPSIdentifiedP"));
    suite.addTest(new PSMultipleTopics("testPSSendReceiveNP"));
    suite.addTest(new PSMultipleTopics("testPSSendReceiveP"));
    suite.addTest(new PSSendSingleTopicDur("testPSSendSingleTopicDurNP"));
    suite.addTest(new PSSendSingleTopicDur("testPSSendSingleTopicDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSUnidentifiedDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSIdentifiedDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSUnidentifiedDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSIdentifiedDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSSendReceiveDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSSendReceiveDurP"));
    suite.addTest(jms.func.nontransacted.clientack.multireceiver.Suite.suite());
    suite.addTest(jms.func.nontransacted.clientack.multisubscriber.Suite.suite());
    return suite;
  }

  public String toString()
  {
    return "Client Acknowledge";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

