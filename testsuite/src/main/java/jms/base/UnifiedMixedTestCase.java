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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.InitialContext;

public class UnifiedMixedTestCase extends JMSTestCase {
    public InitialContext ctx = null;
    ConnectionFactory cf = null;

    public UnifiedMixedTestCase(String name) {
        super(name);
    }

    public Connection createConnection(InitialContext ctx, String lookup, boolean start) {
        Connection qc = null;
        try {
            if (cf == null)
                cf = (ConnectionFactory) ctx.lookup(lookup);
            qc = cf.createConnection();
            if (start)
                qc.start();
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return qc;
    }

    public Connection createConnection(InitialContext ctx, String lookup) {
        Connection qc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            qc = createConnection(ctx, lookup, false);
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return qc;
    }

    public Connection createConnection(String lookup) {
        Connection qc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            qc = createConnection(ctx, lookup);
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return qc;
    }

    public Connection createConnection(String lookup, boolean start) {
        Connection qc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            qc = createConnection(ctx, lookup, start);
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return qc;
    }

    public Connection createConnection(InitialContext ctx, String lookup, String clientId, boolean start) {
        Connection tc = null;
        try {
            if (cf == null)
                cf = (ConnectionFactory) ctx.lookup(lookup);
            tc = cf.createConnection();
            if (clientId != null)
                tc.setClientID(clientId);
            if (start)
                tc.start();
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return tc;
    }

    public Connection createConnection(String lookup, String clientId) {
        Connection tc = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            tc = createConnection(ctx, lookup, clientId, true);
        } catch (Exception e) {
            failFast("create connection failed: " + e);
        }
        return tc;
    }

    public Destination getDestination(String name) {
        Destination dest = null;
        try {
            if (ctx == null)
                ctx = createInitialContext();
            dest = (Destination) ctx.lookup(name);
        } catch (Exception e) {
            failFast("get topic failed: " + e);
        }
        return dest;
    }

    protected void tearDown() throws Exception {
        if (ctx != null)
            ctx.close();
        ctx = null;
        cf = null;
        super.tearDown();
    }
}

