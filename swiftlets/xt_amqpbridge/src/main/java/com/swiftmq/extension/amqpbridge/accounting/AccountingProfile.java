package com.swiftmq.extension.amqpbridge.accounting;

import com.swiftmq.extension.amqpbridge.SwiftletContext;

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
