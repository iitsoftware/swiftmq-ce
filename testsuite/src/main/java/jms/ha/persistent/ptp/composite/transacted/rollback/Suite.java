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

package jms.ha.persistent.ptp.composite.transacted.rollback;

import jms.base.Checker;
import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import javax.jms.JMSException;
import javax.jms.Message;

public class Suite extends ActiveTestSuite {
    public void run(TestResult testResult) {
        super.run(testResult);
    }

    public static Test suite() {
        int maxMsgs = Integer.parseInt(System.getProperty("jms.ha.composite.nmsgs", "20000"));
        TestSuite suite = new Suite();
        suite.addTest(new Receiver("receive", "complink1", maxMsgs));
        suite.addTest(new Receiver("receive", "complink2", maxMsgs));
        suite.addTest(new SelectorReceiver("receive", "complink3", maxMsgs, maxMsgs / 100, new Checker() {
            public boolean isMatch(Message msg) {
                try {
                    String prop = msg.getStringProperty("Prop");
                    return prop != null && prop.equals("X");
                } catch (JMSException e) {
                    return false;
                }
            }
        }));
        suite.addTest(new Sender("send"));
        return suite;
    }

    public String toString() {
        return "rollback";
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(suite());
    }
}

