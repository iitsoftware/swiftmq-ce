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

package com.swiftmq.extension.jmsbridge.accounting;

import com.swiftmq.extension.jmsbridge.SwiftletContext;
import com.swiftmq.swiftlet.accounting.*;

import java.util.HashMap;
import java.util.Map;

public class JMSBridgeSourceFactory implements AccountingSourceFactory {
    SwiftletContext ctx = null;
    Map parms = null;

    public JMSBridgeSourceFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        parms = new HashMap();
        Parameter p = new Parameter("Flush Interval", "Flush Interval of Accounting Data in milliseconds", "60000", false, new ParameterVerifier() {
            public void verify(Parameter parameter, String value) throws InvalidValueException {
                try {
                    long ms = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidValueException(e.toString());
                }
            }
        });
        parms.put(p.getName(), p);
        p = new Parameter("Server Name", "Server Name", null, true, null);
        parms.put(p.getName(), p);
        p = new Parameter("Bridging Name", "Bridging Name", null, true, null);
        parms.put(p.getName(), p);
    }

    public boolean isSingleton() {
        return false;
    }

    public String getGroup() {
        return "JMS Bridge";
    }

    public String getName() {
        return "JMSBridgeSourceFactory";
    }

    public Map getParameters() {
        return parms;
    }

    public AccountingSource create(Map map) throws Exception {
        long flushInterval = Long.parseLong((String) map.get("Flush Interval"));
        String serverName = (String) map.get("Server Name");
        String bridgingName = (String) map.get("Bridging Name");
        return new JMSBridgeSource(ctx, flushInterval,
                new AccountingProfile(ctx, serverName, bridgingName));

    }
}
