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

package amqp.v100.ps.one_to_n.non_durable.dedicatedsession.at_most_once;

import amqp.v100.base.Util;
import amqp.v100.ps.one_to_n.non_durable.dedicatedsession.Receiver;
import amqp.v100.ps.one_to_n.non_durable.dedicatedsession.Sender;
import com.swiftmq.amqp.v100.client.QoS;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

public class Suite extends ActiveTestSuite {
    public static Test suite() {
        int nPairs = Integer.parseInt(System.getProperty("npairs", "10"));
        int nSubscribers = Integer.parseInt(System.getProperty("nsubscribers", "10"));
        TestSuite suite = new Suite();
        for (int i = 0; i < nPairs; i++) {
            for (int j = 0; j < nSubscribers; j++)
                suite.addTest(new Receiver("receive", QoS.AT_MOST_ONCE, Util.getTopicNamePrefix() + (i + 1)));
            suite.addTest(new Sender("send", QoS.AT_MOST_ONCE, Util.getTopicNamePrefix() + (i + 1)));
        }
        return suite;
    }

    public String toString() {
        return "at_most_once";
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(suite());
    }
}

