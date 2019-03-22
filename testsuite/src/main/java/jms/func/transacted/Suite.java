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

package jms.func.transacted;

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
    suite.addTest(new PTPMultipleQueues("testPTPCommitUnidentifiedNP"));
    suite.addTest(new PTPMultipleQueues("testPTPCommitIdentifiedNP"));
    suite.addTest(new PTPMultipleQueues("testPTPCommitUnidentifiedP"));
    suite.addTest(new PTPMultipleQueues("testPTPCommitIdentifiedP"));
    suite.addTest(new PTPMultipleQueues("testPTPCommitSendReceiveNP"));
    suite.addTest(new PTPMultipleQueues("testPTPCommitSendReceiveP"));
    suite.addTest(new PTPMultipleQueues("testPTPRollbackSendReceiveNP"));
    suite.addTest(new PTPMultipleQueues("testPTPRollbackSendReceiveP"));
    suite.addTest(new PSSendSingleTopic("testPSSendSingleTopicNP"));
    suite.addTest(new PSSendSingleTopic("testPSSendSingleTopicP"));
    suite.addTest(new PSSendSingleTopicOM("testPSSendSingleTopicOMNP"));
    suite.addTest(new PSSendSingleTopicOM("testPSSendSingleTopicOMP"));
    suite.addTest(new PSMultipleTopics("testPSCommitUnidentifiedNP"));
    suite.addTest(new PSMultipleTopics("testPSCommitIdentifiedNP"));
    suite.addTest(new PSMultipleTopics("testPSCommitUnidentifiedP"));
    suite.addTest(new PSMultipleTopics("testPSCommitIdentifiedP"));
    suite.addTest(new PSMultipleTopics("testPSCommitSendReceiveNP"));
    suite.addTest(new PSMultipleTopics("testPSCommitSendReceiveP"));
    suite.addTest(new PSMultipleTopics("testPSRollbackSendReceiveNP"));
    suite.addTest(new PSMultipleTopics("testPSRollbackSendReceiveP"));
    suite.addTest(new PSSendSingleTopicDur("testPSSendSingleTopicDurNP"));
    suite.addTest(new PSSendSingleTopicDur("testPSSendSingleTopicDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitUnidentifiedDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitIdentifiedDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitUnidentifiedDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitIdentifiedDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitSendReceiveDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSCommitSendReceiveDurP"));
    suite.addTest(new PSMultipleTopicsDur("testPSRollbackSendReceiveDurNP"));
    suite.addTest(new PSMultipleTopicsDur("testPSRollbackSendReceiveDurP"));
    suite.addTest(jms.func.transacted.multireceiver.Suite.suite());
    suite.addTest(jms.func.transacted.multisubscriber.Suite.suite());
    return suite;
  }

  public String toString()
  {
    return "Transacted";
  }

  public static void main(String args[])
  {
    junit.textui.TestRunner.run(suite());
  }
}

