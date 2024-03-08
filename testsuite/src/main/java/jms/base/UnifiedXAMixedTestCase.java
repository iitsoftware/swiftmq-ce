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

package jms.base;

import javax.jms.Destination;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.naming.InitialContext;

public class UnifiedXAMixedTestCase extends JMSTestCase {
    public InitialContext ctx = null;
    XAConnectionFactory qcf = null;

    public UnifiedXAMixedTestCase(String name) {
        super(name);
    }

    public XAConnection createXAConnection(InitialContext ctx, String lookup) {
        return createXAConnection(ctx, lookup, true);
    }

    public XAConnection createXAConnection(InitialContext ctx, String lookup, boolean start) {
        XAConnection qc = null;
        try {
            if (qcf == null)
                qcf = (XAConnectionFactory) ctx.lookup(lookup);
            qc = qcf.createXAConnection();
            if (start)
                qc.start();
        } catch (Exception e) {
            failFast("create xa connection failed: " + e);
        }
        return qc;
    }

    public XAConnection createXAConnection(String lookup) {

        return createXAConnection(lookup, true);
    }

    public XAConnection createXAConnection(String lookup, boolean start) {
        XAConnection qc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            qc = createXAConnection(ctx, lookup, start);
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return qc;
    }

    public XAConnection createXAConnection(InitialContext ctx, String lookup, String clientId, boolean start) {
        XAConnection tc = null;
        try {
            if (qcf == null)
                qcf = (XAConnectionFactory) ctx.lookup(lookup);
            tc = qcf.createXAConnection();
            if (clientId != null)
                tc.setClientID(clientId);
            if (start)
                tc.start();
        } catch (Exception e) {
            failFast("create xa topic connection failed: " + e);
        }
        return tc;
    }

    public XAConnection createXAConnection(InitialContext ctx, String lookup, String clientId) {
        return createXAConnection(ctx, lookup, clientId, true);
    }

    public XAConnection createXAConnection(String lookup, String clientId, boolean start) {
        XAConnection tc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            tc = createXAConnection(ctx, lookup, clientId, start);
        } catch (Exception e) {
            failFast("create topic connection failed: " + e);
        }
        return tc;
    }

    public XAConnection createXAConnection(String lookup, String clientId) {
        return createXAConnection(lookup, clientId, true);
    }

    public Destination getDestination(String name) {
        Destination dest = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            dest = (Destination) ctx.lookup(name);
        } catch (Exception e) {
            failFast("get dest failed: " + e);
        }
        return dest;
    }

    protected void tearDown() throws Exception {
        if (ctx != null)
            ctx.close();
        ctx = null;
        qcf = null;
        super.tearDown();
    }
}
