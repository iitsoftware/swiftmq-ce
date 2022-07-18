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

package com.swiftmq.impl.mgmt.standard.v750;

import com.swiftmq.impl.mgmt.standard.Dispatcher;
import com.swiftmq.impl.mgmt.standard.SwiftletContext;
import com.swiftmq.impl.mgmt.standard.auth.AuthenticatorImpl;
import com.swiftmq.impl.mgmt.standard.po.EventObject;
import com.swiftmq.impl.mgmt.standard.po.*;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.mgmt.protocol.v750.*;
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
import java.util.*;

public class DispatcherImpl extends ProtocolVisitorAdapter
        implements Dispatcher, EventObjectVisitor {
    static final long LEASE_EXPIRATION = 120000;
    static final int BUCKET_SIZE = 128;
    static final MgmtFactory mfactory = new MgmtFactory();

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
    Set announcedRouters = new HashSet();
    List subscriptionFilter = new ArrayList();
    BulkRequest bulkRequest = new BulkRequest();
    ConnectRequest connectRequest = null;
    boolean started = false;
    boolean authenticated = false;
    Serializable challenge = null;
    Entity usageEntity = null;
    Property leaseTimeoutProp = null;
    Set dirtyContexts = new HashSet();
    String userName = null;
    AuthenticatorImpl authenticator = null;

    public DispatcherImpl(SwiftletContext ctx, String userName, String queueName) {
        this.ctx = ctx;
        this.userName = userName;
        this.queueName = queueName;
        factory = new ProtocolFactory();
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
                ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " from host '" + connectRequest.getHostname() + "' connected.");
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

    private boolean isFilterMatch(String[] context) {
        boolean b = false;
        for (int i = 0; i < subscriptionFilter.size(); i++) {
            if (((SubscriptionFilter) subscriptionFilter.get(i)).isMatch(context)) {
                b = true;
                break;
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isFilterMatch: " + SwiftUtilities.concat(context, "/") + " " + b);
        return b;
    }

    private void createPropertyChangeRequests(Entity entity) {
        String[] context = entity.getContext();
        Map props = entity.getProperties();
        if (props != null && props.size() > 0) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); ) {
                Property p = (Property) ((Map.Entry) iter.next()).getValue();
                Object defaultValue = p.getDefaultValue();
                Object currentValue = p.getValue();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/createPropertyChangeRequests, ctx=" + SwiftUtilities.concat(context, "/") + ", p: " + p.getName() + ", current=" + currentValue + ", default=" + defaultValue);
                if (currentValue != defaultValue ||
                        currentValue != null && defaultValue != null &&
                                !currentValue.equals(defaultValue)) {
                    ensure();
                    requests[length++] = new PropertyChangedRequest(context, p.getName(), currentValue);
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/createPropertyChangeRequests, ctx=" + SwiftUtilities.concat(context, "/") + ", p: " + p.getName() + ", change request created!");
                }
            }
        }
    }

    private Entity getContextEntity(Entity actEntity, String[] context, int stacklevel) {
        if (actEntity == null)
            return null;
        if (actEntity.getName().equals(context[stacklevel])) {
            if (stacklevel == context.length - 1)
                return actEntity;
            else
                return getContextEntity(actEntity.getEntity(context[stacklevel + 1]), context, stacklevel + 1);
        }
        return null;
    }

    private void markContextDirty(String[] context) {
        String c = SwiftUtilities.concat(context, "/");
        if (!dirtyContexts.contains(c)) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/markContextDirty: " + c);
            dirtyContexts.add(c);
        }
    }

    private void markAllDirty(Entity entity) {
        markContextDirty(entity.getContext());
        Map entities = entity.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                markAllDirty(e);
            }
        }
    }

    private boolean isContextDirty(String[] context) {
        String c = SwiftUtilities.concat(context, "/");
        boolean b = dirtyContexts.remove(c);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isContextDirty: " + c + " = " + b);
        return b;
    }

    private boolean roleStrip(Entity entity) throws Exception {
        if (authenticator == null)
            return true;
        return authenticator.roleStrip(entity);
    }

    private RouterConfigInstance copy(RouterConfigInstance source) throws Exception {
        dos.rewind();
        Dumpalizer.dump(dos, source);
        return (RouterConfigInstance) Dumpalizer.construct(new DataByteArrayInputStream(dos), mfactory);
    }

    public void process(EventObject event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/process, event: " + event);
        event.accept(this);
    }

    //--> EventObjectVisitor
    public void visit(EntityAdded event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (!connectRequest.isSubscribeChangeEvents())
            return;
        if (isFilterMatch(event.getContext())) {
            EntityAddedRequest request = new EntityAddedRequest(event.getContext(), event.getName());
            ensure();
            requests[length++] = request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        } else
            markContextDirty(event.getContext());
    }

    public void visit(EntityRemoved event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (!connectRequest.isSubscribeChangeEvents())
            return;
        if (isFilterMatch(event.getContext())) {
            EntityRemovedRequest request = new EntityRemovedRequest(event.getContext(), event.getName());
            ensure();
            requests[length++] = request;
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
        } else
            markContextDirty(event.getContext());
    }

    public void visit(PropertyChanged event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (!connectRequest.isSubscribeChangeEvents())
            return;
        if (isFilterMatch(event.getContext())) {
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
        } else {
            if (event.getEntityListContext() != null)
                markContextDirty(event.getEntityListContext());
        }
    }

    public void visit(SwiftletAdded event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (!connectRequest.isSubscribeChangeEvents())
            return;
        try {
            if (!roleStrip(event.getConfig()))
                return;
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

    public void visit(SwiftletRemoved event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event);
        if (!started)
            return;
        if (!connectRequest.isSubscribeChangeEvents())
            return;
        SwiftletRemovedRequest request = new SwiftletRemovedRequest(event.getName());
        ensure();
        requests[length++] = request;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event: " + event + ", generated: " + request);
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
            e.printStackTrace();
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
        reply.setOk(false);
        reply.setException(new Exception("The old Explorer has retired and isn't supported anymore. Please use SwiftMQ Explorer App of Flow Director."));
        send(reply);
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
                    RouterConfigInstance routerConfigInstance = copy(RouterConfiguration.Singleton());
                    roleStrip(routerConfigInstance);
                    dos.rewind();
                    Dumpalizer.dump(dos, routerConfigInstance);
                    send(new RouterConfigRequest(connectRequest.getConnectId(), SwiftletManager.getInstance().getRouterName(), dos.getBuffer(), dos.getCount()));
                } catch (Exception e) {
                    valid = false;
                    authenticated = false;
                    started = false;
                }
            }
            if (connectRequest.isSubscribeRouteInfos()) {
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
            if (request.isInternal()) {
                if (authenticator == null)
                    reply.setResult(RouterConfiguration.Singleton().executeInternalCommand(request.getContext(), request.getCommand()));
                else
                    reply.setResult(RouterConfiguration.Singleton().executeInternalCommand(request.getContext(), request.getCommand(), authenticator));
            } else {
                if (authenticator == null)
                    reply.setResult(RouterConfiguration.Singleton().executeCommand(request.getContext(), request.getCommand()));
                else
                    reply.setResult(RouterConfiguration.Singleton().executeCommand(request.getContext(), request.getCommand(), authenticator));
            }
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

    public void visit(SetSubscriptionFilterRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        String[] context = request.getContext();
        subscriptionFilter.add(new SubscriptionFilter(context, request.isIncludeNextLevel()));
        Entity entity = getContextEntity(RouterConfiguration.Singleton().getEntity(context[0]), context, 0);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " getContextEntity returns: " + (entity == null ? "null" : entity.getName()));
        if (entity != null) {
            if (entity instanceof EntityList) {
                if (isContextDirty(entity.getContext())) {
                    ensure();
                    requests[length++] = new EntityListClearRequest(context);
                    Map map = entity.getEntities();
                    if (map != null && map.size() > 0) {
                        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                            Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                            ensure();
                            requests[length++] = new EntityAddedRequest(context, e.getName());
                            createPropertyChangeRequests(e);
                            markAllDirty(e);
                        }
                    }
                }
            } else {
                createPropertyChangeRequests(entity);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " done");
    }

    public void visit(RemoveSubscriptionFilterRequest request) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + " ...");
        boolean b = subscriptionFilter.remove(new SubscriptionFilter(request.getContext(), request.isIncludeNextLevel()));
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, request: " + request + ", removed = " + b);
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
        if (connectRequest != null && ctx.connectLoggingEnabled)
            ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " from host '" + connectRequest.getHostname() + "' disconnected. Reason: Lease Timeout");
        send(new DisconnectedRequest(SwiftletManager.getInstance().getRouterName(), "Lease Timeout"));
        started = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doExpire done");
    }

    public void doDisconnect() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doDisconnect ...");
        if (connectRequest != null && ctx.connectLoggingEnabled)
            ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " from host '" + connectRequest.getHostname() + "' disconnected. Reason: Disconnected by Administrator");
        send(new DisconnectedRequest(SwiftletManager.getInstance().getRouterName(), "Disconnected by Administrator"));
        started = false;
        usageEntity = null;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/doDisconnect done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/close ...");
        if (connectRequest != null && ctx.connectLoggingEnabled)
            ctx.logSwiftlet.logInformation(ctx.mgmtSwiftlet.getName(), connectRequest.getToolName() + " from host '" + connectRequest.getHostname() + "' disconnected.");
        try {
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
        return "v750/DispatcherImpl, started=" + started + ", queueName=" + queueName;
    }

    private class SubscriptionFilter {
        String[] context = null;
        boolean includeNextLevel = false;

        private SubscriptionFilter(String[] context, boolean includeNextLevel) {
            this.context = context;
            this.includeNextLevel = includeNextLevel;
        }

        public String[] getContext() {
            return context;
        }

        public boolean isIncludeNextLevel() {
            return includeNextLevel;
        }

        public boolean isMatch(String[] thatContext) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isMatch, this=" + this + ", that=" + SwiftUtilities.concat(thatContext, "/"));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isMatch, context.length == thatContext.length?" + (context.length == thatContext.length));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isMatch, includeNextLevel && context.length == thatContext.length+1?" + (includeNextLevel && context.length == thatContext.length + 1));
            if (context.length == thatContext.length ||
                    includeNextLevel && context.length + 1 == thatContext.length) {
                for (int i = 0; i < context.length; i++) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/isMatch, " + context[i] + ".equals(" + thatContext[i] + ")" + context[i].equals(thatContext[i]));
                    if (!context[i].equals(thatContext[i]))
                        return false;
                }
                return true;
            }
            return false;
        }

        public boolean equals(Object o) {
            SubscriptionFilter that = (SubscriptionFilter) o;
            if (that.includeNextLevel != includeNextLevel)
                return false;
            if (that.context.length != context.length)
                return false;
            for (int i = 0; i < context.length; i++) {
                if (!context[i].equals(that.context[i]))
                    return false;
            }
            return true;
        }

        public String toString() {
            return "[SubscriptionFilter, context=" + SwiftUtilities.concat(context, "/") + ", includeNextLevel=" + includeNextLevel + "]";
        }
    }
}