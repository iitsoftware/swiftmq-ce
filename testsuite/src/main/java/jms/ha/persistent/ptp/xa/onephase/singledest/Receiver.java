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

package jms.ha.persistent.ptp.xa.onephase.singledest;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.Message;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Receiver extends SimpleConnectedXAPTPTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;

    public Receiver(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        verifier = new MsgNoVerifier(this, nMsgs, "no");
//    verifier.setCheckSequence(false);
    }

    public void receive() {
        try {
            sender.close();
            Xid xid = new XidImpl(getClass().getName());
            xares.start(xid, XAResource.TMNOFLAGS);
            for (int i = 0; i < nMsgs; i++) {
                Message msg = receiver.receive(120000);
                if (msg == null)
                    break;
                verifier.add(msg);
                if ((i + 1) % 10 == 0) {
                    xares.end(xid, XAResource.TMSUCCESS);
                    xares.commit(xid, true);
                    if (i + 1 < nMsgs) {
                        xid = new XidImpl(getClass().getName());
                        xares.start(xid, XAResource.TMNOFLAGS);
                    }
                }
            }
            verifier.verify();
        } catch (Exception e) {
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        super.tearDown();
    }
}

