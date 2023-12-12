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
import com.swiftmq.impl.routing.single.connection.event.ActivationListener;
import com.swiftmq.impl.routing.single.connection.stage.ProtocolSelectStage;
import com.swiftmq.impl.routing.single.connection.stage.StageQueue;
import com.swiftmq.impl.routing.single.smqpr.CloseStageQueueRequest;
import com.swiftmq.impl.routing.single.smqpr.SMQRFactory;
import com.swiftmq.impl.routing.single.smqpr.SMQRVisitor;
import com.swiftmq.impl.routing.single.smqpr.StartStageRequest;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ListenerMetaData;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RoutingConnection
        implements RequestService {
    SwiftletContext ctx = null;
    final AtomicReference<Connection> connection = new AtomicReference<>();
    String password = null;
    boolean listener = false;
    InboundReader inboundReader = null;
    OutboundWriter outboundWriter = null;
    SMQRFactory smqrFactory = null;
    final AtomicReference<String> routerName = new AtomicReference<>();
    final AtomicReference<String> protocolVersion = new AtomicReference<>();
    final AtomicLong keepaliveInterval = new AtomicLong();
    final AtomicBoolean closed = new AtomicBoolean(false);
    StageQueue stageQueue = null;
    SMQRVisitor visitor = null;
    String connectionId = null;
    final AtomicInteger transactionSize = new AtomicInteger();
    final AtomicInteger windowSize = new AtomicInteger();
    final AtomicBoolean xa = new AtomicBoolean(true);
    final AtomicBoolean xaSelected = new AtomicBoolean(false);
    Entity entity = null;
    final AtomicReference<Entity> usageEntity = new AtomicReference<>();
    final AtomicReference<ActivationListener> activationListener = new AtomicReference<>();

    public RoutingConnection(SwiftletContext ctx, Connection connection, Entity entity, String password) throws IOException {
        this.ctx = ctx;
        this.connection.set(connection);
        this.entity = entity;
        this.password = password;
        this.listener = connection.getMetaData() instanceof ListenerMetaData;
        boolean compression = false;
        Property cprop = entity.getProperty("use-compression");
        if (cprop != null)
            compression = (Boolean) cprop.getValue();
        Property xprop = entity.getProperty("use-xa");
        if (xprop != null)
            xa.set((Boolean) xprop.getValue());
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

        // Start protocol handshaking (initiated from the connector)
        if (listener)
            setKeepaliveInterval(connection.getMetaData().getKeepAliveInterval());
        else
            stageQueue.enqueue(new StartStageRequest());
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/create done");
    }

    public void setUsageEntity(Entity usageEntity) {
        this.usageEntity.set(usageEntity);
        setProtocolVersion(protocolVersion.get());
        setXaSelected(xaSelected.get());
    }

    public String getProtocolVersion() {
        return protocolVersion.get();
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion.set(protocolVersion);
        try {
            if (usageEntity.get() != null)
                usageEntity.get().getProperty("protocol-version").setValue(protocolVersion);
        } catch (Exception e) {
        }
    }

    public void setXaSelected(boolean xaSelected) {
        this.xaSelected.set(xaSelected);
        try {
            if (usageEntity.get() != null)
                usageEntity.get().getProperty("uses-xa").setValue(new Boolean(xaSelected));
        } catch (Exception e) {
        }
    }

    public ActivationListener getActivationListener() {
        return activationListener.get();
    }

    public void setActivationListener(ActivationListener activationListener) {
        this.activationListener.set(activationListener);
    }

    public Entity getEntity() {
        return entity;
    }

    public int getTransactionSize() {
        return transactionSize.get();
    }

    public void setTransactionSize(int transactionSize) {
        this.transactionSize.set(transactionSize);
    }

    public int getWindowSize() {
        return windowSize.get();
    }

    public void setWindowSize(int windowSize) {
        this.windowSize.set(windowSize);
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
        this.keepaliveInterval.set(keepaliveInterval);
        if (keepaliveInterval > 0) {
            ctx.timerSwiftlet.addTimerListener(keepaliveInterval, inboundReader);
            ctx.timerSwiftlet.addTimerListener(keepaliveInterval, outboundWriter);
        }
    }

    public long getKeepaliveInterval() {
        return keepaliveInterval.get();
    }

    public boolean isXa() {
        return xa.get();
    }

    public Connection getConnection() {
        return connection.get();
    }

    public SMQRVisitor getVisitor() {
        return visitor;
    }

    public SingleProcessorQueue getOutboundQueue() {
        return outboundWriter.getOutboundQueue();
    }

    public void setRouterName(String routerName) {
        this.routerName.set(routerName);
    }

    public String getRouterName() {
        return routerName.get();
    }

    public String getRemoteHostName() {
        return connection.get().getHostname();
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void serviceRequest(Request request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/serviceRequest, request=" + request);
        stageQueue.enqueue(request);
    }

    public void enqueueRequest(Request request) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + " ...");
        if (closed.get()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + ", closed!");
            throw new Exception("RoutingConnection is closed");
        }
        stageQueue.enqueue(request);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueRequest, request=" + request + " done");
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void close() {
        if (closed.getAndSet(true))
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close...");
        if (keepaliveInterval.get() > 0) {
            ctx.timerSwiftlet.removeTimerListener(inboundReader);
            ctx.timerSwiftlet.removeTimerListener(outboundWriter);
        }
        Semaphore sem = new Semaphore();
        CloseStageQueueRequest r = new CloseStageQueueRequest();
        r.setSemaphore(sem);
        stageQueue.enqueue(r);
        sem.waitHere();
        outboundWriter.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "[RoutingConnection " + routerName + "|" + connectionId + "]";
    }
}
