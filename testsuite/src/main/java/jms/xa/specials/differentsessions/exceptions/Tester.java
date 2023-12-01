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

package jms.xa.specials.differentsessions.exceptions;

import jms.base.MultisessionConnectedXAPTPTestCase;
import jms.base.XidImpl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class Tester extends MultisessionConnectedXAPTPTestCase {
    public Tester(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp(5);
    }

    public void testP() {
        try {
            Xid xid1 = new XidImpl();
            Xid xid2 = new XidImpl();
            XAResource xares1 = sessions[0].getXAResource();
            XAResource xares2 = sessions[1].getXAResource();
            XAResource xares3 = sessions[2].getXAResource();
            xares1.start(xid1, XAResource.TMNOFLAGS);
            try {
                xares2.start(xid1, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                assertTrue("e.errorCode != XAException.XAER_DUPID", e.errorCode == XAException.XAER_DUPID);
            }
            try {
                xares1.start(xid2, XAResource.TMNOFLAGS);
            } catch (XAException e) {
                assertTrue("e.errorCode != XAException.XAER_RMERR", e.errorCode == XAException.XAER_RMERR);
            }
            try {
                xares3.start(xid2, XAResource.TMJOIN);
            } catch (XAException e) {
                assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
            }
            try {
                xares3.prepare(xid2);
            } catch (XAException e) {
                assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
            }
            try {
                xares3.rollback(xid2);
            } catch (XAException e) {
                assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
            }
            try {
                xares3.prepare(xid1);
            } catch (XAException e) {
                e.printStackTrace();
                assertTrue("e.errorCode != XAException.XAER_NOTA", e.errorCode == XAException.XAER_NOTA);
            }
        } catch (Exception e) {
            e.printStackTrace();
            failFast("test failed: " + e);
        }
    }
}
