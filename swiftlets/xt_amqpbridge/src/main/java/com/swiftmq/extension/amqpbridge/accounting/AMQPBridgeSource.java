package com.swiftmq.extension.amqpbridge.accounting;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.accounting.AccountingSource;
import com.swiftmq.swiftlet.accounting.StopListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.util.IdGenerator;

import java.net.InetAddress;
import java.util.Date;

public class AMQPBridgeSource implements AccountingSource, TimerListener {
    private static final String PROP_SWIFTLET = "swiftlet";
    private static final String PROP_TIMESTAMP = "timestamp";
    private static final String PROP_ROUTERNAME = "routername";
    private static final String PROP_ROUTERHOSTNAME = "routerhostname";

    SwiftletContext ctx = null;
    StopListener stopListener = null;
    AccountingSink accountingSink = null;
    long flushInterval = 0;
    AccountingProfile accountingProfile = null;
    String uniqueueId = IdGenerator.getInstance().nextId('-');
    long count = 0;
    String routerName = null;
    String routerHostName = null;

    public AMQPBridgeSource(SwiftletContext ctx, long flushInterval, AccountingProfile accountingProfile) {
        this.ctx = ctx;
        this.flushInterval = flushInterval;
        this.accountingProfile = accountingProfile;
        routerName = SwiftletManager.getInstance().getRouterName();
        try {
            routerHostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            routerHostName = "unknown";
        }
        accountingProfile.setSource(this);
    }

    public void performTimeAction() {
        ctx.bridgeSwiftlet.flushAccounting(accountingProfile.getBridgeType(), accountingProfile.getBridgeName());
    }

    public void setStopListener(StopListener stopListener) {
        this.stopListener = stopListener;
    }

    public synchronized void startAccounting(AccountingSink accountingSink) throws Exception {
        this.accountingSink = accountingSink;
        ctx.bridgeSwiftlet.startAccounting(accountingProfile.getBridgeType(), accountingProfile.getBridgeName(), accountingProfile);
        ctx.timerSwiftlet.addTimerListener(flushInterval, this);
    }

    public synchronized void stopAccounting() throws Exception {
        accountingSink = null;
        ctx.bridgeSwiftlet.stopAccounting(accountingProfile.getBridgeType(), accountingProfile.getBridgeName());
        ctx.timerSwiftlet.removeTimerListener(this);
    }

    public synchronized void send(BridgeCollector collector) {
        if (accountingSink != null) {
            try {
                MapMessageImpl msg = new MapMessageImpl();
                msg.setJMSMessageID(uniqueueId + (count++));
                if (count == Long.MAX_VALUE)
                    count = 0;
                msg.setJMSTimestamp(System.currentTimeMillis());
                msg.setString(PROP_SWIFTLET, ctx.bridgeSwiftlet.getName());
                msg.setStringProperty(PROP_SWIFTLET, ctx.bridgeSwiftlet.getName());
                msg.setString(PROP_TIMESTAMP, BridgeCollector.fmt.format(new Date()));
                msg.setString(PROP_ROUTERNAME, routerName);
                msg.setStringProperty(PROP_ROUTERNAME, routerName);
                msg.setString(PROP_ROUTERHOSTNAME, routerHostName);
                msg.setStringProperty(PROP_ROUTERHOSTNAME, routerHostName);
                collector.dumpToMapMessage(msg);
                accountingSink.add(msg);
            } catch (Exception e) {
                if (stopListener != null)
                    stopListener.sourceStopped(this, e);
            }
        }
    }

}
