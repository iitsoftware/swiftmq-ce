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

package com.swiftmq.impl.routing.single.connection;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.accounting.AccountingProfile;
import com.swiftmq.impl.routing.single.connection.event.ActivationListener;
import com.swiftmq.impl.routing.single.connection.stage.ProtocolSelectStage;
import com.swiftmq.impl.routing.single.connection.stage.StageQueue;
import com.swiftmq.impl.routing.single.smqpr.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;

import java.io.IOException;

public class RoutingConnection
        implements RequestService {
    SwiftletContext ctx = null;
    Connection connection = null;
    String password = null;
    boolean listener = false;
    InboundReader inboundReader = null;
    OutboundWriter outboundWriter = null;
    SMQRFactory smqrFactory = null;
    String routerName = null;
    String protocolVersion = null;
    long keepaliveInterval = 0;
    boolean closed = false;
    StageQueue stageQueue = null;
    SMQRVisitor visitor = null;
    String connectionId = null;
    int transactionSize = 0;
    int windowSize = 0;
    boolean xa = true;
    boolean xaSelected = false;
    Entity entity = null;
    Entity usageEntity = null;
    ActivationListener activationListener = null;
    AccountingProfile accountingProfile = null;

    public RoutingConnection(SwiftletContext ctx, Connection connection, Entity entity, String password) throws IOException {
        this.ctx = ctx;
        this.connection = connection;
        this.entity = entity;
        this.password = password;
        this.listener = connection.getMetaData() instanceof ListenerMetaData;
        boolean compression = false;
        Property cprop = entity.getProperty("use-compression");
        if (cprop != null)
            compression = ((Boolean) cprop.getValue()).booleanValue();
        Property xprop = entity.getProperty("use-xa");
        if (xprop != null)
            xa = ((Boolean) xprop.getValue()).booleanValue();
        connectionId = connection.toString();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/create...");
        outboundWriter = new OutboundWriter(ctx, this, compression);
        smqrFactory = new SMQRFactory();
        inboundReader = new InboundReader(ctx, this, smqrFactory, compression);
        inboundReader.addRequestService(this);
        connection.setInboundHandler(inboundReader);
        visitor = new SMQRVisitor(ctx, this);
        stageQueue = new StageQueue(ctx);
        stageQueue.setStage(new ProtocolSelectStage(ctx, this, listener));
        stageQueue.startQueue();

        AccountingProfile ap = ctx.routingSwiftlet.getAccountingProfile();
        if (ap != null)
            startAccounting(ap);

        // Start protocol handshaking (initiated from the connector)
        if (listener)
            setKeepaliveInterval(connection.getMetaData().getKeepAliveInterval());
        else
            stageQueue.enqueue(new StartStageRequest());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/create done");
    }

    public synchronized void setUsageEntity(Entity usageEntity) {
        this.usageEntity = usageEntity;
        setProtocolVersion(protocolVersion);
        setXaSelected(xaSelected);
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public synchronized void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
        try {
            if (usageEntity != null)
                usageEntity.getProperty("protocol-version").setValue(protocolVersion);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void setXaSelected(boolean xaSelected) {
        this.xaSelected = xaSelected;
        try {
            if (usageEntity != null)
                usageEntity.getProperty("uses-xa").setValue(new Boolean(xaSelected));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ActivationListener getActivationListener() {
        return activationListener;
    }

    public void setActivationListener(ActivationListener activationListener) {
        this.activationListener = activationListener;
    }

    public Entity getEntity() {
        return entity;
    }

    public int getTransactionSize() {
        return transactionSize;
    }

    public void setTransactionSize(int transactionSize) {
        this.transactionSize = transactionSize;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public SMQRFactory getSMQRFactory() {
        return smqrFactory;
    }

    public String getPassword() {
        return password;
    }

    public boolean isListener() {
        return listener;
    }

    public void setKeepaliveInterval(long keepaliveInterval) {
        this.keepaliveInterval = keepaliveInterval;
        if (keepaliveInterval > 0) {
            ctx.timerSwiftlet.addTimerListener(keepaliveInterval, inboundReader);
            ctx.timerSwiftlet.addTimerListener(keepaliveInterval, outboundWriter);
        }
    }

    public long getKeepaliveInterval() {
        return keepaliveInterval;
    }

    public boolean isXa() {
        return xa;
    }

    public Connection getConnection() {
        return connection;
    }

    public SMQRVisitor getVisitor() {
        return visitor;
    }

    public SingleProcessorQueue getOutboundQueue() {
        return outboundWriter.getOutboundQueue();
    }

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }

    public String getRouterName() {
        return routerName;
    }

    public String getRemoteHostName() {
        return connection.getHostname();
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void startAccounting(AccountingProfile accountingProfile) {
        if (this.accountingProfile == null) {
            this.accountingProfile = accountingProfile;
            serviceRequest(new StartAccountingRequest(accountingProfile));
        }
    }

    public void flushAccounting() {
        if (accountingProfile != null)
            serviceRequest(new FlushAccountingRequest());
    }

    public void stopAccounting() {
        if (accountingProfile != null)
            serviceRequest(new StopAccountingRequest());
        accountingProfile = null;
    }

    public void serviceRequest(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/serviceRequest, request=" + request);
        stageQueue.enqueue(request);
    }

    public synchronized void enqueueRequest(Request request) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + " ...");
        if (closed) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + ", closed!");
            throw new Exception("RoutingConnection is closed");
        }
        stageQueue.enqueue(request);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + " done");
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    public synchronized void close() {
        if (closed)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close...");
        if (keepaliveInterval > 0) {
            ctx.timerSwiftlet.removeTimerListener(inboundReader);
            ctx.timerSwiftlet.removeTimerListener(outboundWriter);
        }
        Semaphore sem = new Semaphore();
        CloseStageQueueRequest r = new CloseStageQueueRequest();
        r.setSemaphore(sem);
        stageQueue.enqueue(r);
        sem.waitHere();
        outboundWriter.close();
        closed = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "[RoutingConnection " + routerName + "|" + connectionId + "]";
    }
}
