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

package jms.ha.persistent.ptp.browser;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedPTPTestCase;

import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import java.util.Enumeration;

public class Browser extends SimpleConnectedPTPTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.browser.nmsgs", "50000"));
    String queueName = null;
    MsgNoVerifier verifier = null;
    QueueBrowser browser = null;

    public Browser(String name, String queueName) {
        super(name);
        this.queueName = queueName;
    }

    protected void setUp() throws Exception {
        setUp(false, Session.AUTO_ACKNOWLEDGE, false, false);
        browser = qs.createBrowser(getQueue(queueName));
        verifier = new MsgNoVerifier(this, nMsgs, "no");
    }

    public void browse() {
        try {
            for (Enumeration _enum = browser.getEnumeration(); _enum.hasMoreElements(); ) {
                Message msg = (Message) _enum.nextElement();
                if (msg == null)
                    throw new Exception("null message received!");
                verifier.add(msg);
            }
            verifier.verify();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        browser.close();
        verifier = null;
        browser = null;
        super.tearDown();
    }
}

