package com.swiftmq.extension.amqpbridge.v091;

import com.swiftmq.extension.amqpbridge.SwiftletContext;
import com.swiftmq.extension.amqpbridge.accounting.AccountingProfile;
import com.swiftmq.extension.amqpbridge.accounting.BridgeCollector091;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.Date;

public class BridgeController implements ExceptionListener, TimerListener {
    SwiftletContext ctx = null;
    Entity bridgeEntity = null;
    ConnectionHolder source = null;
    ConnectionHolder target = null;
    boolean enabled = false;
    boolean connected = false;
    long retryInterval;
    Entity usageEntity = null;
    boolean disconnectInProgress = false;
    AccountingProfile accountingProfile = null;
    BridgeCollector091 bridgeCollector = null;

    public BridgeController(final SwiftletContext ctx, Entity bridgeEntity) {
        this.ctx = ctx;
        this.bridgeEntity = bridgeEntity;

        usageEntity = ((EntityList) ctx.usage.getEntity("bridges091")).createEntity();
        usageEntity.setName(bridgeEntity.getName());
        usageEntity.setDynamicObject(this);
        usageEntity.createCommands();
        try {
            ctx.usage.getEntity("bridges091").addEntity(usageEntity);
        } catch (EntityAddException e) {
        }
        Property prop = usageEntity.getProperty("connecttime");
        try {
            prop.setValue("DISCONNECTED");
        } catch (Exception e) {
        }
        prop.setReadOnly(true);

        prop = bridgeEntity.getProperty("enabled");
        enabled = (Boolean) prop.getValue();
        try {
            if (enabled)
                connect();
        } catch (NoClassDefFoundError noClassDefFoundError) {
        }
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                enabled = (Boolean) newValue;
                if (enabled)
                    try {
                        connect();
                    } catch (NoClassDefFoundError noClassDefFoundError) {
                        throw new PropertyChangeException("RabbitMQ Java client classes not found. Please install them, see docs!");
                    }
                else
                    disconnect();
            }
        });

        prop = bridgeEntity.getProperty("retryinterval");
        retryInterval = (Long) prop.getValue();
        if (retryInterval > 0)
            ctx.timerSwiftlet.addTimerListener(retryInterval, BridgeController.this);
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                if (retryInterval > 0)
                    ctx.timerSwiftlet.removeTimerListener(BridgeController.this);
                retryInterval = (Long) newValue;
                if (retryInterval > 0)
                    ctx.timerSwiftlet.addTimerListener(retryInterval, BridgeController.this);
            }
        });
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/created");
    }

    private void initBridgeCollector() {
        Entity sourceEntity = bridgeEntity.getEntity("source");
        bridgeCollector.setSourceConnectURI((String) sourceEntity.getProperty("connect-uri").getValue());
        bridgeCollector.setSourceExchangeName((String) sourceEntity.getProperty("exchange-name").getValue());
        bridgeCollector.setSourceExchangeType((String) sourceEntity.getProperty("exchange-type").getValue());
        bridgeCollector.setSourceConsumerTag((String) sourceEntity.getProperty("consumer-tag").getValue());
        bridgeCollector.setSourceQueueName((String) sourceEntity.getProperty("queue-name").getValue());
        bridgeCollector.setSourceRoutingKey((String) sourceEntity.getProperty("routing-key").getValue());
        Entity targetEntity = bridgeEntity.getEntity("target");
        bridgeCollector.setTargetConnectURI((String) targetEntity.getProperty("connect-uri").getValue());
        bridgeCollector.setTargetExchangeName((String) targetEntity.getProperty("exchange-name").getValue());
        bridgeCollector.setTargetRoutingKey((String) targetEntity.getProperty("routing-key").getValue());
    }

    private void createCollector() {
        bridgeCollector = new BridgeCollector091(bridgeEntity.getName());
        if (connected)
            initBridgeCollector();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "createCollector, bridgeCollector=" + bridgeCollector);
    }

    public void startAccounting(AccountingProfile accountingProfile) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "startAccounting, this.accountingProfile=" + this.accountingProfile);
        if (this.accountingProfile == null) {
            this.accountingProfile = accountingProfile;
            createCollector();
            if (target != null)
                target.setBridgeCollector091(bridgeCollector);
        }
    }

    public void flushAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "flushAccounting, accountingProfile=" + accountingProfile + ", bridgeCollector=" + bridgeCollector);
        if (accountingProfile != null && bridgeCollector != null && bridgeCollector.isDirty())
            accountingProfile.getSource().send(bridgeCollector);
    }

    public void stopAccounting() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), "stopAccounting, accountingProfile=" + accountingProfile);
        if (target != null)
            target.setBridgeCollector091(null);
        if (accountingProfile != null) {
            accountingProfile = null;
            bridgeCollector = null;
        }
    }

    public void onException(Exception e) {
        if (disconnectInProgress)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/onException, e=" + e);
        disconnectInProgress = true;
        ctx.timerSwiftlet.addInstantTimerListener(500, new TimerListener() {
            public void performTimeAction() {
                disconnect();
            }
        });
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/performTimeAction");
        try {
            if (!connected && enabled)
                connect();
        } catch (NoClassDefFoundError noClassDefFoundError) {
        }
    }

    private synchronized void connect() throws NoClassDefFoundError {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect ...");
        try {
            source = new ConnectionHolder(ctx, bridgeEntity.getEntity("source"));
            target = new ConnectionHolder(ctx, bridgeEntity.getEntity("target"));
            if (bridgeCollector != null) {
                initBridgeCollector();
                target.setBridgeCollector091(bridgeCollector);
            }

            target.createConnection();
            source.createAndRunConnection(target, this);
            connected = true;
            Property prop = usageEntity.getProperty("connecttime");
            prop.setValue(new Date().toString());
            prop.setReadOnly(true);
            ctx.logSwiftlet.logInformation(ctx.bridgeSwiftlet.getName(), toString() + "/connected");
            target.setUsageEntity(usageEntity);
        } catch (Throwable e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect, exception=" + e);
            ctx.logSwiftlet.logError(ctx.bridgeSwiftlet.getName(), toString() + "/unable to connect, exception=" + e);
            if (source != null) {
                source.close();
                source = null;
            }
            if (target != null) {
                target.close();
                target = null;
            }
            if (e instanceof java.lang.NoClassDefFoundError)
                throw (NoClassDefFoundError) e;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/connect done");
    }

    private synchronized void disconnect() {
        if (!connected)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/disconnect ...");
        ctx.logSwiftlet.logInformation(ctx.bridgeSwiftlet.getName(), toString() + "/disconnecting");
        if (source != null) {
            source.close();
            source = null;
        }
        if (target != null) {
            target.close();
            target = null;
        }
        Property prop = usageEntity.getProperty("connecttime");
        try {
            prop.setValue("DISCONNECTED");
        } catch (Exception e) {
        }
        prop.setReadOnly(true);
        disconnectInProgress = false;
        connected = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/disconnect done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.bridgeSwiftlet.getName(), toString() + "/close");
        enabled = false;
        flushAccounting();
        disconnect();
        ctx.usage.getEntity("bridges-091").removeDynamicEntity(this);
        if (retryInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);
    }

    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("BridgeController");
        sb.append(" name=").append(bridgeEntity.getName());
        sb.append(", source=").append(source);
        sb.append(", target=").append(target);
        sb.append(", enabled=").append(enabled);
        sb.append(", connected=").append(connected);
        return sb.toString();
    }
}
