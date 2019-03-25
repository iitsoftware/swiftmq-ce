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

package com.swiftmq.impl.jms.standard.v400;

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.impl.jms.standard.JMSSwiftlet;
import com.swiftmq.impl.jms.standard.SwiftletContext;
import com.swiftmq.impl.jms.standard.VersionedJMSConnection;
import com.swiftmq.impl.jms.standard.accounting.AccountingProfile;
import com.swiftmq.jms.smqp.v400.*;
import com.swiftmq.jms.v400.ConnectionMetaDataImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.InboundHandler;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.requestreply.GenericRequest;
import com.swiftmq.tools.requestreply.Reply;
import com.swiftmq.tools.requestreply.Request;
import com.swiftmq.tools.requestreply.RequestService;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.JMSException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class JMSConnection
        implements RequestService, VersionedJMSConnection {
    SwiftletContext ctx = null;
    boolean closed = false;
    boolean started = false;
    Connection connection;
    protected InboundReader inboundReader;
    protected OutboundWriter outboundWriter;
    protected ActiveLogin activeLogin;
    protected String tracePrefix;
    String tpPrefix;
    ArrayList tmpQueues = new ArrayList();
    ConnectionVisitor visitor = null;
    String clientId = null;
    String remoteHostname = null;
    boolean authenticated = false;
    String userName = null;
    Serializable challenge = null;
    protected String connectionId = null;
    Entity connectionEntity = null;
    Property receivedSecProp = null;
    Property sentSecProp = null;
    EntityList tmpQueueEntityList = null;
    EntityList sessionEntityList = null;
    ThreadPool myTp = null;
    long keepAliveInterval = 0;
    boolean smartTree = false;
    ConnectionQueue connectionQueue = null;

    public JMSConnection(SwiftletContext ctx, Entity connectionEntity, Connection connection) {
        this.ctx = ctx;
        this.connectionEntity = connectionEntity;
        this.connection = connection;
        smartTree = SwiftletManager.getInstance().isUseSmartTree();
        if (!smartTree) {
            tmpQueueEntityList = (EntityList) connectionEntity.getEntity("tempqueues");
            sessionEntityList = (EntityList) connectionEntity.getEntity("sessions");
        }
        receivedSecProp = connectionEntity.getProperty("msgs-received");
        sentSecProp = connectionEntity.getProperty("msgs-sent");
        keepAliveInterval = connection.getMetaData().getKeepAliveInterval();
        connectionId = "v400/" + connection.toString();
        remoteHostname = connection.getHostname();

        visitor = new ConnectionVisitor();

        tpPrefix = "sys$jms/JMSConnection " + connectionId;
        tracePrefix = "JMSConnection " + connectionId;

        outboundWriter = new OutboundWriter(connection);
        inboundReader = new InboundReader(tracePrefix);
        inboundReader.addRequestService(this); // Connection service
        inboundReader.setReplyHandler(outboundWriter);

        myTp = ctx.threadpoolSwiftlet.getPool(JMSSwiftlet.TP_CONNSVC);

        connectionQueue = new ConnectionQueue(myTp, this);
        connectionQueue.startQueue();
        if (keepAliveInterval > 0)
            ctx.timerSwiftlet.addTimerListener(keepAliveInterval, outboundWriter);
    }

    public InboundHandler getInboundHandler() {
        return inboundReader;
    }

    public void sendReply(Reply reply) {
        outboundWriter.performReply(reply);
    }

    public boolean isClosed() {
        return (closed);
    }

    public void collect(long lastCollectTime) {
        connectionQueue.enqueue(new CollectRequest(lastCollectTime));
    }

    public void close() {
        connectionQueue.enqueue(new GenericRequest(0, false, null));
    }

    public void startAccounting(AccountingProfile accountingProfile) {
        ctx.logSwiftlet.logWarning(tracePrefix, "This SMQP protocol version (v400) does NOT support accounting! Please upgrade this client to use accounting!");
    }

    public void flushAccounting() {
        // no-op
    }

    public void stopAccounting() {
        // no-op
    }

    protected Session createSession(CreateSessionRequest req, int sessionDispatchId, Entity sessionEntity) {
        Session session = null;
        switch (req.getType()) {
            case CreateSessionRequest.QUEUE_SESSION:
                if (req.isTransacted())
                    session = new TransactedQueueSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin);
                else
                    session = new NontransactedQueueSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin, req.getAcknowledgeMode());
                break;
            case CreateSessionRequest.TOPIC_SESSION:
                if (req.isTransacted())
                    session = new TransactedTopicSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin);
                else
                    session = new NontransactedTopicSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin, req.getAcknowledgeMode());
                break;
            case CreateSessionRequest.UNIFIED:
                if (req.isTransacted())
                    session = new TransactedUnifiedSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin);
                else
                    session = new NontransactedUnifiedSession(tracePrefix, sessionEntity, outboundWriter.getOutboundQueue(), sessionDispatchId, activeLogin, req.getAcknowledgeMode());
                break;
        }
        return session;
    }

    public void serviceRequest(Request request) {
        connectionQueue.enqueue(request);
    }

    public String toString() {
        return connectionId;
    }

    class ConnectionVisitor extends ConnectionVisitorAdapter {
        public void visitCloseSessionRequest(CloseSessionRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/CLOSE SESSION");
            CloseSessionReply reply = (CloseSessionReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            Session session = (Session) inboundReader.getRequestService(req.getSessionDispatchId());
            inboundReader.removeRequestService(req.getSessionDispatchId());
            req.sem = new Semaphore();
            session.serviceRequest(req);
            req.sem.waitHere();
            reply.setOk(true);
            if (!smartTree)
                sessionEntityList.removeDynamicEntity(session);
            activeLogin.getResourceLimitGroup().decSessions();
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending session close reply");
            reply.send();
        }

        public void visitCreateSessionRequest(CreateSessionRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/CREATE SESSION");
            CreateSessionReply reply = (CreateSessionReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            try {
                activeLogin.getResourceLimitGroup().incSessions();
                int sessionDispatchId = inboundReader.getNextFreeDispatchId();
                Session session = null;
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/creating new Session");
                if (smartTree)
                    session = createSession(req, sessionDispatchId, null);
                else {
                    Entity sessionEntity = sessionEntityList.createEntity();
                    session = createSession(req, sessionDispatchId, sessionEntity);
                    sessionEntity.setName(String.valueOf(sessionDispatchId));
                    sessionEntity.setDynamicObject(session);
                    sessionEntity.createCommands();
                    Property prop = sessionEntity.getProperty("transacted");
                    prop.setValue(new Boolean(req.isTransacted()));
                    prop.setReadOnly(true);
                    prop = sessionEntity.getProperty("acknowledgemode");
                    prop.setValue(SwiftUtilities.ackModeToString(req.getAcknowledgeMode()));
                    prop.setReadOnly(true);
                    sessionEntityList.addEntity(sessionEntity);
                }
                inboundReader.addRequestService(session);
                reply.setOk(true);
                reply.setSessionDispatchId(sessionDispatchId);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending sessionDispatchId: " + sessionDispatchId);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/exception creating new JMSSession: " + e);
                ctx.logSwiftlet.logError("sys$jms", tracePrefix + "/exception creating new JMSSession: " + e);
                reply.setOk(false);
                reply.setException(new javax.jms.JMSException(e.toString()));
            }
            reply.send();
        }

        public void visitCreateTmpQueueRequest(CreateTmpQueueRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/CREATE TEMP QUEUE");
            CreateTmpQueueReply reply = (CreateTmpQueueReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            try {
                activeLogin.getResourceLimitGroup().incTempQueues();
                String tmpQueueName = ctx.queueManager.createTemporaryQueue();
                tmpQueues.add(tmpQueueName);
                reply.setOk(true);
                reply.setQueueName(tmpQueueName);
                if (!smartTree) {
                    Entity tqe = tmpQueueEntityList.createEntity();
                    tqe.setName(tmpQueueName);
                    tmpQueueEntityList.addEntity(tqe);
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending temp queue name: " + tmpQueueName);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/exception creating temp queue: " + e);
                ctx.logSwiftlet.logError("sys$jms", tracePrefix + "/exception creating temp queue: " + e);
                reply.setOk(false);
                reply.setException(new javax.jms.JMSException(e.toString()));
            }
            reply.send();
        }

        public void visitDeleteTmpQueueRequest(DeleteTmpQueueRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/DELETE TEMP QUEUE");
            DeleteTmpQueueReply reply = (DeleteTmpQueueReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            try {
                String tmpQueueName = req.getQueueName();
                ctx.queueManager.deleteTemporaryQueue(tmpQueueName);
                ctx.jndiSwiftlet.deregisterJNDIQueueObject(tmpQueueName);
                tmpQueues.remove(tmpQueueName);
                reply.setOk(true);
                if (!smartTree)
                    tmpQueueEntityList.removeEntity(tmpQueueEntityList.getEntity(tmpQueueName));
                activeLogin.getResourceLimitGroup().decTempQueues();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/deleted temp queue name: " + tmpQueueName);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", tracePrefix + "/exception deleting temp queue: " + e);
                reply.setOk(false);
                reply.setException(new javax.jms.JMSException(e.toString()));
            }
            reply.send();
        }

        public void visitGetClientIdRequest(GetClientIdRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/GET CLIENT ID");
            GetClientIdReply reply = (GetClientIdReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            reply.setOk(true);
            reply.setClientId(activeLogin.getLoginId().toString());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending clientID: " + reply.getClientId());
            reply.send();
        }

        public void visitSetClientIdRequest(SetClientIdRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/SET CLIENT ID");
            SetClientIdReply reply = (SetClientIdReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            clientId = req.getClientId() + "@" + SwiftletManager.getInstance().getRouterName();
            try {
                ctx.authSwiftlet.verifySetClientId(activeLogin.getLoginId());
                ctx.jmsSwiftlet.addClientId(clientId);
                activeLogin.setClientId(clientId);
                reply.setOk(true);
                reply.setClientId(clientId);
                Property prop = connectionEntity.getProperty("clientid");
                prop.setReadOnly(false);
                prop.setValue(clientId);
                prop.setReadOnly(true);
            } catch (Exception e) {
                clientId = null;
                reply.setOk(false);
                reply.setException((e instanceof JMSException) ? e : new javax.jms.JMSException(e.toString()));
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending clientID: " + reply.getClientId());
            reply.send();
        }

        public void visitGetMetaDataRequest(GetMetaDataRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/GET META DATA");
            ConnectionMetaDataImpl cmd = new ConnectionMetaDataImpl();
            GetMetaDataReply reply = (GetMetaDataReply) req.createReply();
            if (!authenticated) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSSecurityException("not authenticated"));
                reply.send();
                return;
            }
            reply.setOk(true);
            reply.setConnectionMetaData(cmd);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", tracePrefix + "/sending ConnectionMetaData: " + cmd);
            reply.send();
        }

        public void visitGetAuthChallengeRequest(GetAuthChallengeRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/GET AUTH CHALLENGE");
            GetAuthChallengeReply reply = (GetAuthChallengeReply) req.createReply();
            try {
                userName = req.getUserName();
                ctx.authSwiftlet.verifyHostLogin(userName, remoteHostname);
                ChallengeResponseFactory crFactory = ctx.jmsSwiftlet.getChallengeResponseFactory();
                String password = ctx.authSwiftlet.getPassword(userName);
                challenge = crFactory.createChallenge(password);
                reply.setOk(true);
                reply.setChallenge(challenge);
                reply.setChallengeResponseFactoryClass(ctx.jmsSwiftlet.getChallengeResponseFactory().getClass().getName());
            } catch (Exception e) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSException(e.toString()));
            }
            reply.send();
        }

        public void visitAuthResponseRequest(AuthResponseRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/AUTH RESPONSE");
            AuthResponseReply reply = (AuthResponseReply) req.createReply();
            try {
                ChallengeResponseFactory crFactory = ctx.jmsSwiftlet.getChallengeResponseFactory();
                String password = ctx.authSwiftlet.getPassword(userName);
                if (crFactory.verifyResponse(challenge, req.getResponse(), password)) {
                    reply.setOk(true);
                    activeLogin = ctx.authSwiftlet.createActiveLogin(userName, "JMS");
                    authenticated = true;
                    Property prop = connectionEntity.getProperty("clientid");
                    prop.setReadOnly(false);
                    prop.setValue(activeLogin.getLoginId().toString());
                    prop.setReadOnly(true);
                    prop = connectionEntity.getProperty("username");
                    prop.setValue(userName);
                    prop.setReadOnly(true);
                    prop = connectionEntity.getProperty("logintime");
                    prop.setValue(new Date().toString());
                    prop.setReadOnly(true);
                } else {
                    reply.setOk(false);
                    reply.setException(new javax.jms.JMSSecurityException("invalid password"));
                }
            } catch (Exception e) {
                reply.setOk(false);
                reply.setException(new javax.jms.JMSException(e.toString()));
            }
            reply.send();
        }

        public void visitDisconnectRequest(DisconnectRequest req) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/DISCONNECT");

            if (keepAliveInterval > 0) {
                ctx.timerSwiftlet.removeTimerListener(outboundWriter);
                keepAliveInterval = 0;
            }

            ctx.logSwiftlet.logInformation("sys$jms", tracePrefix + "/receiving disconnect request, scheduling connection close");
            DisconnectReply reply = (DisconnectReply) req.createReply();
            reply.setOk(true);
            reply.send();
        }

        public void visitGenericRequest(GenericRequest req) {
            if (closed)
                return;
            closed = true;

            if (keepAliveInterval > 0) {
                ctx.timerSwiftlet.removeTimerListener(outboundWriter);
                keepAliveInterval = 0;
            }

            if (clientId != null)
                ctx.jmsSwiftlet.removeClientId(clientId);

            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/closing connection");

            // delete all temp queues
            for (int i = 0; i < tmpQueues.size(); i++) {
                try {
                    String tmpQueueName = (String) tmpQueues.get(i);
                    ctx.queueManager.deleteTemporaryQueue(tmpQueueName);
                    ctx.jndiSwiftlet.deregisterJNDIQueueObject(tmpQueueName);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace("sys$jms", tracePrefix + "/delete tmp queue, got exception: " + e);
                }
            }
            tmpQueues.clear();

            // close all sessions
            for (int i = 1; i < inboundReader.getNumberServices(); i++) {
                Session session = (Session) inboundReader.getRequestService(i);
                if (session != null) {
                    CloseSessionRequest request = new CloseSessionRequest(0);
                    request.sem = new Semaphore();
                    session.serviceRequest(request);
                    request.sem.waitHere();
                }
                inboundReader.removeRequestService(i);
            }
            inboundReader.removeRequestService(0);
            connectionQueue.stopQueue();
            connectionQueue.clear();
            ctx.logSwiftlet.logInformation("sys$jms", tracePrefix + "/connection closed");
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", tracePrefix + "/closing connection DONE.");
        }

        public void visit(CollectRequest collectRequest) {
            if (closed)
                return;

            long received = 0;
            long sent = 0;
            for (int i = 1; i < inboundReader.getNumberServices(); i++) {
                Session session = (Session) inboundReader.getRequestService(i);
                if (session != null) {
                    received += session.ctx.getMsgsReceived();
                    sent += session.ctx.getMsgsSent();
                }
            }
            double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - collectRequest.getLastCollect()) / 1000.0);
            double rsec = ((double) received / (double) deltasec) + 0.5;
            double ssec = ((double) sent / (double) deltasec) + 0.5;
            try {
                receivedSecProp.setValue(new Integer((int) rsec));
                sentSecProp.setValue(new Integer((int) ssec));
            } catch (Exception e) {
            }
        }
    }
}

