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

package jms.ha.persistent.ptp.cluster.xa.onephase.singledest;

import jms.ha.persistent.ptp.cluster.xa.onephase.Sender;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class Suite extends ActiveTestSuite {
    public void run(TestResult testResult) {
        super.run(testResult);
    }

    public static Test suite() {
        TestSuite suite = new Suite();
        suite.addTest(new Receiver("receive"));
        suite.addTest(new Sender("send", System.getProperty("jndi.cluster.queue", "cqueue")));
        return suite;
    }

    public String toString() {
        return "singledest";
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(suite());
    }
}

