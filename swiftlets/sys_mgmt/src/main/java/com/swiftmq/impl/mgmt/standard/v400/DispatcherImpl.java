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

package com.swiftmq.impl.mgmt.standard.v400;

import com.swiftmq.impl.mgmt.standard.Dispatcher;
import com.swiftmq.impl.mgmt.standard.SwiftletContext;
import com.swiftmq.impl.mgmt.standard.po.*;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.RouterConfiguration;
import com.swiftmq.mgmt.protocol.v400.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.routing.Route;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.DeliveryMode;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class DispatcherImpl extends ProtocolVisitorAdapter
        implements Dispatcher, EventObjectVisitor {
    static final long LEASE_EXPIRATION = 60000;
    static final int BUCKET_SIZE = 128;

    SwiftletContext ctx = null;
    ProtocolFactory factory = null;
    QueueSender queueSender = null;
    String queueName = null;
    QueueImpl replyQueue = null;
    long leaseExpiration = 0;
    boolean valid = false;
    Object[] requests = null;
    Map changedProps = new HashMap();
    int length = 0;
    DataByteArrayInputStream dis = new DataByteArrayInputStream();
    DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
    HashSet announcedRouters = new HashSet();
    BulkRequest bulkRequest = new BulkRequest();
    ConnectRequest connectRequest = null;
    boolean started = false;
    boolean authenticated = false;
    Serializable challenge = null;
    Entity usageEntity = null;
    Property leaseTimeoutProp = null;

    public DispatcherImpl(SwiftletContext ctx, String queueName) {
        this.ctx = ctx;
        this.queueName = queueName;
        factory = new com.swiftmq.mgmt.protocol.v400.ProtocolFactory();
        bulkRequest.factory = factory;
        requests = new Object[BUCKET_SIZE];
        length = 0;
        try {
            queueSender = ctx.queueManager.createQueueSender(queueName, null);
            replyQueue = new QueueImpl(queueName);
            valid = true;
            leaseExpiration = System.currentTimeMillis() + LEASE_EXPIRATION;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/created");
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/create, got exception: " + e);
            ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString() + "/create, got exception: " + e);
        }
    }

    private void createUsageEntity() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/createUsageEntity ...");
        try {
            usageEntity = ((EntityList) ctx.usageList).createEntity();
            usageEntity.setName(queueName);
            usageEntity.createCommands();
            usageEntity.setDynamic(true);
            leaseTimeoutProp = usageEntity.getProperty("lease-timeout");
            leaseTimeoutProp.setValue(new Date(leaseExpiration).toString());
            usageEntity.getProperty("hostname").setValue(connectRequest.getHostname());
            usageEntity.getProperty("toolname").setValue(connectRequest.getToolName());
            usageEntity.getProperty("connecttime").setValue(new Date().toString());
            ctx.usageList.addEntity(usageEntity);
            if (ctx.connectLoggingEnabled)
                ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " connected from host '" + connectRequest.getHostname() + "'");
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/createUsageEntity done");
    }

    private void ensure() {
        if (length == requests.length - 1) {
            Object[] o = new Object[requests.length + BUCKET_SIZE];
            System.arraycopy(requests, 0, o, 0, length);
            requests = o;
        }
    }

    private void send(Dumpable dumpable) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/send, dumpable: " + dumpable);
        try {
            BytesMessageImpl msg = new BytesMessageImpl();
            msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            msg.setJMSDestination(replyQueue);
            msg.setJMSPriority(MessageImpl.MAX_PRIORITY - 1);
            dos.rewind();
            Dumpalizer.dump(dos, dumpable);
            msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
            QueuePushTransaction t = queueSender.createTransaction();
            t.putMessage(msg);
            t.commit();
        } catch (Exception e) {
            valid = false;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/send, got exception: " + e);
        }
    }

    public void process(EventObject event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/process, event: " + event);
        try {
            event.accept(this);
        } catch (Exception e) {
        }
    }

    //--> EventObjectVisitor
    public void visit(EntityAdded event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeChangeEvents()) {
            EntityAddedRequest request = new EntityAddedRequest(event.getContext(), event.getName());
            ensure();
            requests[length++] = request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        }
    }

    public void visit(EntityRemoved event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeChangeEvents()) {
            EntityRemovedRequest request = new EntityRemovedRequest(event.getContext(), event.getName());
            ensure();
            requests[length++] = request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        }
    }

    public void visit(SwiftletAdded event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeChangeEvents()) {
            try {
                dos.rewind();
                Dumpalizer.dump(dos, event.getConfig());
                SwiftletAddedRequest request = new SwiftletAddedRequest(event.getName(), dos.getBuffer(), dos.getCount());
                ensure();
                requests[length++] = request;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", exception: " + e);
                ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", exception: " + e);
                valid = false;
            }
        }
    }

    public void visit(SwiftletRemoved event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeChangeEvents()) {
            SwiftletRemovedRequest request = new SwiftletRemovedRequest(event.getName());
            ensure();
            requests[length++] = request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        }
    }

    public void visit(PropertyChanged event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeChangeEvents()) {
            String key = SwiftUtilities.concat(event.getContext(), "/") + "/" + event.getName();
            PropertyChangedRequest request = (PropertyChangedRequest) changedProps.get(key);
            if (request != null)
                request.setValue(event.getValue());
            else {
                request = new PropertyChangedRequest(event.getContext(), event.getName(), event.getValue());
                ensure();
                requests[length++] = request;
                changedProps.put(key, request);
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        }
    }

    public void visit(RouterAvailable event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeRouteInfos()) {
            if (!announcedRouters.contains(event.getName())) {
                announcedRouters.add(event.getName());
                send(new RouterAvailableRequest(event.getName()));
            }
        }
    }

    public void visit(RouterUnavailable event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (connectRequest.isSubscribeRouteInfos()) {
            if (announcedRouters.contains(event.getName())) {
                announcedRouters.remove(event.getName());
                send(new RouterUnavailableRequest(event.getName()));
            }
        }
    }

    public void visit(ClientRequest event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        try {
            dis.reset();
            dis.setBuffer(event.getBuffer());
            Request request = (Request) Dumpalizer.construct(dis, factory);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", request: " + request);
            request.accept(this);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", exception: " + e);
            ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", exception: " + e);
            valid = false;
        }
    }
    //<-- EventObjectVisitor

    //--> ProtocolVisitorAdapter

    public void visit(ConnectRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        connectRequest = request;
        ConnectReply reply = (ConnectReply) request.createReply();
        reply.setOk(true);
        reply.setRouterName(SwiftletManager.getInstance().getRouterName());
        reply.setLeaseTimeout(LEASE_EXPIRATION);
        reply.setAuthRequired(ctx.authEnabled);
        if (ctx.authEnabled) {
            reply.setCrFactory(ctx.challengeResponseFactory.getClass().getName());
            challenge = ctx.challengeResponseFactory.createChallenge(ctx.password);
            reply.setChallenge(challenge);
        } else {
            authenticated = true;
            started = true;
            createUsageEntity();
        }
        send(reply);
        if (started) {
            if (connectRequest.isSubscribeRouterConfig()) {
                try {
                    dos.rewind();
                    Dumpalizer.dump(dos, RouterConfiguration.Singleton());
                    send(new RouterConfigRequest(connectRequest.getConnectId(), SwiftletManager.getInstance().getRouterName(), dos.getBuffer(), dos.getCount()));
                } catch (Exception e) {
                    valid = false;
                    authenticated = false;
                    started = false;
                }
            }
            if (connectRequest.isSubscribeRouteInfos() && ctx.routingSwiftlet != null) {
                try {
                    Route[] routes = ctx.routingSwiftlet.getRoutes();
                    if (routes != null) {
                        for (int i = 0; i < routes.length; i++) {
                            if (routes[i].isActive()) {
                                send(new RouterAvailableRequest(routes[i].getDestination()));
                                announcedRouters.add(routes[i].getDestination());
                            }
                        }
                    }
                } catch (Exception e) {
                    valid = false;
                    authenticated = false;
                    started = false;
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " done");
    }

    public void visit(AuthRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        AuthReply reply = (AuthReply) request.createReply();
        if (ctx.challengeResponseFactory.verifyResponse(challenge, request.getResponse(), ctx.password)) {
            reply.setOk(true);
            authenticated = true;
            started = true;
            createUsageEntity();
        } else {
            reply.setOk(false);
            reply.setException(new Exception("Authentication failed - invalid password!"));
        }
        send(reply);
        if (started) {
            if (connectRequest.isSubscribeRouterConfig()) {
                try {
                    dos.rewind();
                    Dumpalizer.dump(dos, RouterConfiguration.Singleton());
                    send(new RouterConfigRequest(connectRequest.getConnectId(), SwiftletManager.getInstance().getRouterName(), dos.getBuffer(), dos.getCount()));
                } catch (Exception e) {
                    valid = false;
                    authenticated = false;
                    started = false;
                }
            }
            if (connectRequest.isSubscribeRouteInfos() && ctx.routingSwiftlet != null) {
                try {
                    Route[] routes = ctx.routingSwiftlet.getRoutes();
                    if (routes != null) {
                        for (int i = 0; i < routes.length; i++) {
                            if (routes[i].isActive()) {
                                send(new RouterAvailableRequest(routes[i].getDestination()));
                                announcedRouters.add(routes[i].getDestination());
                            }
                        }
                    }
                } catch (Exception e) {
                    valid = false;
                    authenticated = false;
                    started = false;
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " done");
    }

    public void visit(CommandRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        CommandReply reply = (CommandReply) request.createReply();
        if (!authenticated) {
            reply.setOk(false);
            reply.setException(new Exception("This router requires password authentication before you can manage it!"));
        } else if (!started) {
            reply.setOk(false);
            reply.setException(new Exception("Protocol Error. Please connect before you submit commands!"));
        } else {
            reply.setOk(true);
            if (request.isInternal())
                reply.setResult(RouterConfiguration.Singleton().executeInternalCommand(request.getContext(), request.getCommand()));
            else
                reply.setResult(RouterConfiguration.Singleton().executeCommand(request.getContext(), request.getCommand()));
        }
        send(reply);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " done");
    }

    public void visit(LeaseRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        leaseExpiration = System.currentTimeMillis() + LEASE_EXPIRATION;
        try {
            if (leaseTimeoutProp != null)
                leaseTimeoutProp.setValue(new Date(leaseExpiration).toString());
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " done");
    }
    //<-- ProtocolVisitorAdapter

    public void flush() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flush ...");
        if (length == 0)
            return;
        if (length == 1)
            send((Request) requests[0]);
        else {
            bulkRequest.dumpables = requests;
            bulkRequest.len = length;
            send(bulkRequest);
        }
        length = 0;
        changedProps.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flush done");
    }

    public boolean isInvalid() {
        return !valid;
    }

    public boolean isExpired() {
        return leaseExpiration < System.currentTimeMillis();
    }

    public void doExpire() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doExpire ...");
        try {
            if (ctx.connectLoggingEnabled)
                ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " disconnected from host '" + connectRequest.getHostname() + "'. Reason: Lease Timeout");
            send(new DisconnectedRequest(SwiftletManager.getInstance().getRouterName(), "Lease Timeout"));
        } catch (Exception e) {
        } finally {
            started = false;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doExpire done");
    }

    public void doDisconnect() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doDisconnect ...");
        try {
            if (ctx.connectLoggingEnabled)
                ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " disconnected from host '" + connectRequest.getHostname() + "'. Reason: Disconnected by Administrator");
            send(new DisconnectedRequest(SwiftletManager.getInstance().getRouterName(), "Disconnected by Administrator"));
        } catch (Exception e) {
        } finally {
            started = false;
            usageEntity = null;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doDisconnect done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/close ...");
        try {
            if (ctx.connectLoggingEnabled)
                ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " disconnected from host '" + connectRequest.getHostname() + "'");
            queueSender.close();
        } catch (Exception e) {
        }
        try {
            if (usageEntity != null)
                ctx.usageList.removeEntity(usageEntity);
        } catch (Exception e) {
        }
        started = false;
        valid = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "v400/DispatcherImpl, started=" + started + ", queueName=" + queueName;
    }
}
