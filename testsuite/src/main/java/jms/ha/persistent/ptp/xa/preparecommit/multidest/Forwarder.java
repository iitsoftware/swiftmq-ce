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

package jms.ha.persistent.ptp.xa.preparecommit.multidest;

import jms.base.MsgNoVerifier;
import jms.base.SimpleConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Forwarder extends SimpleConnectedXAPTPTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.nmsgs", "100000"));
    MsgNoVerifier verifier = null;
    String sourceQueueName = null;
    String targetQueueName = null;
    QueueSender mySender = null;
    QueueReceiver myReceiver = null;

    public Forwarder(String name, String sourceQueueName, String targetQueueName) {
        super(name);
        this.sourceQueueName = sourceQueueName;
        this.targetQueueName = targetQueueName;
    }

    protected void setUp() throws Exception {
        super.setUp();
        sender.close();
        receiver.close();
        mySender = qs.getQueueSession().createSender(getQueue(targetQueueName));
        myReceiver = qs.getQueueSession().createReceiver(getQueue(sourceQueueName));
        verifier = new MsgNoVerifier(this, nMsgs, "no");
//    verifier.setCheckSequence(false);
    }

    public void forward() {
        try {
            for (int i = 0; i < nMsgs; i++) {
                Xid xid = new XidImpl(getClass().getName());
                xares.start(xid, XAResource.TMNOFLAGS);
                Message msg = myReceiver.receive(120000);
                if (msg == null)
                    break;
                verifier.add(msg);
                mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                xares.end(xid, XAResource.TMSUCCESS);
                xares.prepare(xid);
                xares.commit(xid, false);
            }
            verifier.verify();
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        verifier = null;
        sourceQueueName = null;
        targetQueueName = null;
        mySender = null;
        myReceiver = null;
        super.tearDown();
    }

}

