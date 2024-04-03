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

package jms.ha.persistent.ptp.cluster.xa.onephase;

import jms.base.SimpleConnectedXAPTPClusterTestCase;
import jms.base.XidImpl;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.QueueSender;
import javax.jms.TextMessage;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Sender extends SimpleConnectedXAPTPClusterTestCase {
    int nMsgs = Integer.parseInt(System.getProperty("jms.ha.cluster.nmsgs", "20000"));
    String queueName = null;
    QueueSender mySender = null;

    public Sender(String name, String queueName) {
        super(name);
        this.queueName = queueName;
    }

    protected void setUp() throws Exception {
        super.setUp();
        sender.close();
        receiver.close();
        mySender = qs.getQueueSession().createSender(getQueue(queueName));
    }

    public void send() {
        try {
            TextMessage msg = qs.createTextMessage();
            for (int i = 0; i < nMsgs; i++) {
                Xid xid = new XidImpl(getClass().getName());
                xares.start(xid, XAResource.TMNOFLAGS);
                msg.setIntProperty("no", i);
                msg.setText("Msg: " + i);
                mySender.send(msg, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                xares.end(xid, XAResource.TMSUCCESS);
                xares.commit(xid, true);
            }

        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }

    protected void tearDown() throws Exception {
        queueName = null;
        mySender = null;
        super.tearDown();
    }
}

