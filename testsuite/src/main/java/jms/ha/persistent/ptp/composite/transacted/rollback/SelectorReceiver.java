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
import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.Message;
import javax.jms.QueueReceiver;
import javax.jms.Session;

public class SelectorReceiver extends SimpleConnectedPTPTestCase {
    int nMsgs = 0;
    int maxMsgs = 0;
    MsgNoVerifier verifier = null;
    String myQueueName = null;
    QueueReceiver myReceiver = null;
    Checker checker = null;

    public SelectorReceiver(String name, String myQueueName, int maxMsgs, int nMsgs, Checker checker) {
        super(name);
        this.myQueueName = myQueueName;
        this.maxMsgs = maxMsgs;
        this.nMsgs = nMsgs;
        this.checker = checker;
    }

    protected void setUp() throws Exception {
        setUp(true, Session.AUTO_ACKNOWLEDGE, false, false);
        myReceiver = qs.createReceiver(getQueue(myQueueName));
        verifier = new MsgNoVerifier(this, maxMsgs, "no");
        verifier.setCheckSequence(false);
        verifier.setMissingOk(true);
    }

    public void receive() {
        try {
            boolean rollback = false;
            int n = 0, m = 0;
            while (n < nMsgs) {
                Message msg = myReceiver.receive();
                if (msg == null)
                    throw new Exception("null message received!");
                if (rollback) {
                    System.out.println("during rollback, ignore: " + msg.getIntProperty("no"));
                    m++;
                    if (m == 10) {
                        System.out.println("rollback session!");
                        qs.rollback();
                        rollback = false;
                        m = 0;
                    }
                    continue;
                }
                if (!checker.isMatch(msg))
                    throw new Exception("Message doesn't match: " + msg);
                System.out.println("accepted: " + msg.getIntProperty("no"));
                verifier.add(msg);
                n++;
                if (n % 10 == 0) {
                    System.out.println("commit session!");
                    qs.commit();
                    rollback = true;
                }
            }
            verifier.verify();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        myReceiver.close();
        super.tearDown();
    }
}
