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

package com.swiftmq.impl.amqpbridge.accounting;

import com.swiftmq.impl.amqpbridge.SwiftletContext;

public class AccountingProfile {
    SwiftletContext ctx = null;
    String tracePrefix = "AccountingProfile";
    AMQPBridgeSource source = null;
    String bridgeType = null;
    String bridgeName = null;

    public AccountingProfile(SwiftletContext ctx, String bridgeType, String bridgeName) {
        this.ctx = ctx;
        this.bridgeType = bridgeType;
        this.bridgeName = bridgeName;
    }

    public AMQPBridgeSource getSource() {
        return source;
    }

    public void setSource(AMQPBridgeSource source) {
        this.source = source;
    }

    public String getBridgeType() {
        return bridgeType;
    }

    public String getBridgeName() {
        return bridgeName;
    }

    private boolean dbg(String s, boolean b) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), tracePrefix + "/" + s + ": " + b);
        return b;
    }

    public boolean isMatchBridge(String bridgingName) {
        return dbg("isMatchBridging", bridgingName.equals(this.bridgeName));
    }

    public String toString() {
        return "[AccountingProfile, source=" + source +
                ", bridgeType=" + bridgeType + ", bridgeName=" + bridgeName + "]";
    }
}
