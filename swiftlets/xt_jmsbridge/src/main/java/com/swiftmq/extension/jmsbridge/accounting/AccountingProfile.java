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

public class AccountingProfile {
    SwiftletContext ctx = null;
    String tracePrefix = "AccountingProfile";
    JMSBridgeSource source = null;
    String serverName = null;
    String bridgingName = null;

    public AccountingProfile(SwiftletContext ctx, String serverName, String bridgingName) {
        this.ctx = ctx;
        this.serverName = serverName;
        this.bridgingName = bridgingName;
    }

    public JMSBridgeSource getSource() {
        return source;
    }

    public void setSource(JMSBridgeSource source) {
        this.source = source;
    }

    public String getServerName() {
        return serverName;
    }

    public String getBridgingName() {
        return bridgingName;
    }

    private boolean dbg(String s, boolean b) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + s + ": " + b);
        return b;
    }

    public boolean isMatchServer(String serverName) {
        return dbg("isMatchServer", serverName.equals(this.serverName));
    }

    public boolean isMatchBridging(String bridgingName) {
        return dbg("isMatchBridging", bridgingName.equals(this.bridgingName));
    }

    public String toString() {
        return "[AccountingProfile, source=" + source +
                ", serverName=" + serverName + ", bridgingName=" + bridgingName + "]";
    }
}
