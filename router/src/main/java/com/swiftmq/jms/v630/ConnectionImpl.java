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

package com.swiftmq.jms.v630;

import com.swiftmq.auth.ChallengeResponseFactory;
import com.swiftmq.client.thread.PoolManager;
import com.swiftmq.jms.*;
import com.swiftmq.jms.smqp.SMQPVersionRequest;
import com.swiftmq.jms.smqp.v630.*;
import com.swiftmq.jms.v630.po.POReconnect;
import com.swiftmq.net.client.ExceptionHandler;
import com.swiftmq.net.client.InboundHandler;
import com.swiftmq.net.client.Reconnector;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.collection.ListSet;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.*;
import com.swiftmq.tools.timer.TimerEvent;
import com.swiftmq.tools.timer.TimerListener;
import com.swiftmq.tools.timer.TimerRegistry;
import com.swiftmq.tools.util.DataStreamOutputStream;
import com.swiftmq.tools.util.LengthCaptureDataInput;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.*;
import javax.jms.IllegalStateException;
import javax.jms.Queue;
import java.io.IOException;
import java.util.*;

public class ConnectionImpl extends RequestServiceRegistry
        implements SwiftMQConnection, Connection, ReplyHandler, RequestHandler, TimerListener, InboundHandler, ExceptionHandler, RecreatableConnection {
    public static final String DISPATCH_TOKEN = "sys$jms.client.connection.connectiontask";

    public static final int CLIENT_VERSION = 630;
    public static final int DISCONNECTED = -1;
    public static final int CONNECTED_STOPPED = 0;
    public static final int CONNECTED_STARTED = 1;
    public static final int INITIAL_KEEPALIVE_COUNT = 5;

    KeepAliveRequest keepaliveRequest = new KeepAliveRequest();

    boolean closed = false;
    int connectionState = DISCONNECTED;
    ConnectionMetaDataImpl metaData = null;
    String clientID = null;
    String internalCID = null;
    String originalCID = null;
    boolean clientIdAdministratively = false;
    String myHostname = null;
    ExceptionListener exceptionListener = null;
    RequestRegistry requestRegistry = null;
    List sessionList = Collections.synchronizedList(new ArrayList());
    List connectionConsumerList = Collections.synchronizedList(new ArrayList());
    Map tmpQueues = Collections.synchronizedMap(new HashMap());
    DumpableFactory dumpableFactory = new com.swiftmq.jms.smqp.SMQPFactory(new com.swiftmq.jms.smqp.v630.SMQPFactory());
    boolean cancelled = false;
    boolean clientIdAllowed = true;
    ChallengeResponseFactory crFactory = null;
    String userName = null;
    String password = null;
    com.swiftmq.net.client.Connection connection = null;
    long keepaliveInterval = 0;
    int smqpProducerReplyInterval = 0;
    int smqpConsumerCacheSize = 0;
    int jmsDeliveryMode = 0;
    int jmsPriority = 0;
    long jmsTTL = 0;
    boolean jmsMessageIdEnabled = false;
    boolean jmsMessageTimestampEnabled = false;
    boolean useThreadContextCL = false;
    boolean duplicateMessageDetection = false;
    int duplicateBacklogSize = 500;
    ListSet duplicateLog = new ListSet(500);
    ConnectionQueue connectionQueue = null;
    ConnectionTask connectionTask = null;
    ThreadPool connectionPool = null;
    DataStreamOutputStream outStream = null;
    Reconnector reconnector = null;
    GetAuthChallengeReply authReply = null;
    Connector connector = null;
    List reconnectListeners = new ArrayList();
    boolean reconnectInProgress = false;
    volatile long lastConnectionLost = -1;
    volatile int connectionId = -1;
    volatile int keepaliveCount = INITIAL_KEEPALIVE_COUNT;
    volatile JMSSecurityException lastSecurityException = null;

    protected ConnectionImpl(String userName, String password, Reconnector reconnector)
            throws JMSException {
        this.userName = userName;
        this.password = password;
        this.reconnector = reconnector;
        connector = new Connector(reconnector);
        reconnectAndWait();
        if (connection == null) {
            if (lastSecurityException != null)
                throw lastSecurityException;
            else
                throw new JMSException("Unable to create a connection to: " + reconnector.getServers());
        } else
            connectionState = CONNECTED_STOPPED;
        connectionPool = PoolManager.getInstance().getConnectionPool();
        connectionTask = new ConnectionTask();
        connectionQueue = new ConnectionQueue();
        connectionQueue.startQueue();
        requestRegistry = new RequestRegistry();
        requestRegistry.setRequestTimeoutEnabled(!reconnector.isEnabled());
        requestRegistry.setRequestHandler(this);
        setReplyHandler(this);
    }

    public boolean isReconnectEnabled() {
        return reconnector.isEnabled();
    }

    private void resetSessions(boolean reset) {
        for (int i = 0; i < sessionList.size(); i++) {
            SessionImpl session = (SessionImpl) sessionList.get(i);
            session.setResetInProgress(reset);
            if (!reset && connectionState == CONNECTED_STARTED)
                session.startSession();
        }
        for (int i = 0; i < connectionConsumerList.size(); i++) {
            ConnectionConsumerImpl cc = (ConnectionConsumerImpl) connectionConsumerList.get(i);
            cc.setResetInProgress(reset);
        }
    }

    private void setSessionBlockState(boolean blocked) {
        for (int i = 0; i < sessionList.size(); i++) {
            SessionImpl session = (SessionImpl) sessionList.get(i);
            session.setBlocked(blocked);
        }
    }

    private void reconnect() {
        if (reconnector.isDebug())
            System.out.println(toString() + ": initiate reconnect...");
        lastConnectionLost = System.currentTimeMillis();
        POReconnect po = new POReconnect(null, this);
        connector.dispatch(po);
    }

    private void reconnectAndWait() {
        if (reconnector.isDebug())
            System.out.println(toString() + ": initiate reconnect and wait ...");
        lastConnectionLost = System.currentTimeMillis();
        Semaphore sem = new Semaphore();
        POReconnect po = new POReconnect(sem, this);
        connector.dispatch(po);
        sem.waitHere();
        if (reconnector.isDebug())
            System.out.println(toString() + ": reconnect done, connection =" + connection);
    }

    public long getLastConnectionLost() {
        return lastConnectionLost;
    }

    public int getConnectionId() {
        return connection == null ? -1 : connectionId;
    }

    public void prepareForReconnect() {
        if (connectionQueue != null) {
            connectionQueue.stopQueue();
            connectionQueue.clear();
        }
        if (requestRegistry != null)
            requestRegistry.setPaused(true);
        resetSessions(true);
        if (connection != null) {
            reconnector.invalidateConnection();
            connection = null;
        }
    }

    public Request getVersionRequest() {
        return new SMQPVersionRequest(CLIENT_VERSION);
    }

    public void setVersionReply(Reply reply) throws Exception {
        if (!reply.isOk())
            throw reply.getException();
    }

    public Request getAuthenticateRequest() {
        return new GetAuthChallengeRequest(0, userName);
    }

    public void setAuthenticateReply(Reply reply) throws Exception {
        if (!reply.isOk()) {
            lastSecurityException = new JMSSecurityException(reply.getException().getMessage());
            throw lastSecurityException;
        }
        lastSecurityException = null;
        authReply = (GetAuthChallengeReply) reply;
        crFactory = (ChallengeResponseFactory) Class.forName(authReply.getFactoryClass()).newInstance();
    }

    public Request getAuthenticateResponse() {
        byte[] challenge = authReply.getChallenge();
        byte[] response = crFactory.createBytesResponse(challenge, password);
        return new AuthResponseRequest(0, response);
    }

    public void setAuthenticateResponseReply(Reply reply) throws Exception {
        if (!reply.isOk()) {
            lastSecurityException = new JMSSecurityException(reply.getException().getMessage());
            throw lastSecurityException;
        }
        lastSecurityException = null;
    }

    public Request getMetaDataRequest() {
        return new GetMetaDataRequest();
    }

    public void setMetaDataReply(Reply reply) throws Exception {
        GetMetaDataReply mdReply = (GetMetaDataReply) reply;
        metaData = mdReply.getMetaData();
    }

    public Request getGetClientIdRequest() {
        return internalCID == null ? null : new GetClientIdRequest(0);
    }

    public void setGetClientIdReply(Reply reply) throws Exception {
        internalCID = ((GetClientIdReply) reply).getClientId();
    }

    public Request getSetClientIdRequest() {
        if (clientID == null)
            return null;
        String s = clientID;
        if (clientID.indexOf('@') != -1)
            s = clientID.substring(0, clientID.indexOf('@'));
        return new SetClientIdRequest(0, s);
    }

    public void setSetClientIdReply(Reply reply) throws Exception {
        if (!reply.isOk())
            throw reply.getException();
        clientID = ((SetClientIdReply) reply).getClientId();
    }

    public Request getRecreateRequest() {
        return null;
    }

    public void setRecreateReply(Reply reply) {
    }

    public List getRecreatables() {
        List list = new ArrayList();
        for (Iterator iter = tmpQueues.entrySet().iterator(); iter.hasNext(); ) {
            list.add(new TemporaryQueueRecreator(this, (QueueImpl) ((Map.Entry) iter.next()).getValue()));
        }
        for (int i = 0; i < sessionList.size(); i++) {
            list.add(sessionList.get(i));
        }
        for (int i = 0; i < connectionConsumerList.size(); i++) {
            list.add(connectionConsumerList.get(i));
        }
        return list;
    }

    public void handOver(com.swiftmq.net.client.Connection connection) {
        this.connection = connection;
        if (connection != null) {
            connectionId++;
            myHostname = connection.getLocalHostname();
            connection.setInboundHandler(this);
            connection.setExceptionHandler(this);
            outStream = new DataStreamOutputStream(connection.getOutputStream());
            if (connectionQueue != null) {
                connectionQueue.clear();
                connectionQueue.startQueue();
            }
            setSessionBlockState(true);   // Blocks new XA Requests
            if (requestRegistry != null) {
                requestRegistry.setPaused(false);
                Semaphore retrySem = new Semaphore();
                requestRegistry.retryAllRequests(retrySem);
                retrySem.waitHere();
            }
            resetSessions(false);
            notifyReconnectListener();
            setSessionBlockState(false);
        }
    }

    private void notifyReconnectListener() {
        if (reconnectListeners.size() == 0)
            return;
        new Thread() {
            public void run() {
                synchronized (reconnectListeners) {
                    for (int i = 0; i < reconnectListeners.size(); i++) {
                        ((ReconnectListener) reconnectListeners.get(i)).reconnected(connection.getHostname(), connection.getPort());
                    }
                }
            }
        }.start();
    }

    public String getUserName() {
        return userName;
    }

    void assignClientId(String clientId) throws JMSException {
        this.clientID = clientId;
        originalCID = clientId;

        if (clientID != null) {
            clientIdAdministratively = true;

            try {
                SetClientIdReply reply =
                        (SetClientIdReply) requestRegistry.request(new SetClientIdRequest(0, clientID));

                if (reply.isOk()) {
                    clientID = reply.getClientId();
                } else {
                    throw new JMSException(reply.getException().toString());
                }
            } catch (Exception e) {
                if (e instanceof JMSException)
                    throw (JMSException) e;
                e.printStackTrace();
            }
        } else {
            clientIdAdministratively = false;

            try {
                GetClientIdReply reply =
                        (GetClientIdReply) requestRegistry.request(new GetClientIdRequest());

                if (reply.isOk()) {
                    internalCID = reply.getClientId();
                } else {
                    throw new JMSException(reply.getException().toString());
                }
            } catch (Exception e) {
                if (e instanceof JMSException)
                    throw (JMSException) e;
                e.printStackTrace();
            }
        }
    }

    public String getInternalCID() {
        return internalCID;
    }

    int getSmqpProducerReplyInterval() {
        return smqpProducerReplyInterval;
    }

    void setSmqpProducerReplyInterval(int smqpProducerReplyInterval) {
        this.smqpProducerReplyInterval = smqpProducerReplyInterval;
    }

    int getSmqpConsumerCacheSize() {
        return smqpConsumerCacheSize;
    }

    void setSmqpConsumerCacheSize(int smqpConsumerCacheSize) {
        this.smqpConsumerCacheSize = smqpConsumerCacheSize;
    }

    int getJmsDeliveryMode() {
        return jmsDeliveryMode;
    }

    void setJmsDeliveryMode(int jmsDeliveryMode) {
        this.jmsDeliveryMode = jmsDeliveryMode;
    }

    int getJmsPriority() {
        return jmsPriority;
    }

    void setJmsPriority(int jmsPriority) {
        this.jmsPriority = jmsPriority;
    }

    long getJmsTTL() {
        return jmsTTL;
    }

    void setJmsTTL(long jmsTTL) {
        this.jmsTTL = jmsTTL;
    }

    boolean isJmsMessageIdEnabled() {
        return jmsMessageIdEnabled;
    }

    void setJmsMessageIdEnabled(boolean jmsMessageIdEnabled) {
        this.jmsMessageIdEnabled = jmsMessageIdEnabled;
    }

    boolean isJmsMessageTimestampEnabled() {
        return jmsMessageTimestampEnabled;
    }

    void setJmsMessageTimestampEnabled(boolean jmsMessageTimestampEnabled) {
        this.jmsMessageTimestampEnabled = jmsMessageTimestampEnabled;
    }

    boolean isUseThreadContextCL() {
        return useThreadContextCL;
    }

    void setUseThreadContextCL(boolean useThreadContextCL) {
        this.useThreadContextCL = useThreadContextCL;
    }

    public void setDuplicateMessageDetection(boolean duplicateMessageDetection) {
        this.duplicateMessageDetection = duplicateMessageDetection;
    }

    public boolean isDuplicateMessageDetection() {
        return duplicateMessageDetection;
    }

    public synchronized void setDuplicateBacklogSize(int duplicateBacklogSize) {
        this.duplicateBacklogSize = duplicateBacklogSize;
        duplicateLog.resize(duplicateBacklogSize);
    }

    public void addReconnectListener(ReconnectListener listener) {
        synchronized (reconnectListeners) {
            reconnectListeners.add(listener);
        }
    }

    public void removeReconnectListener(ReconnectListener listener) {
        synchronized (reconnectListeners) {
            reconnectListeners.remove(listener);
        }
    }

    void startKeepAlive(long keepaliveInterval) {
        this.keepaliveInterval = keepaliveInterval;
        TimerRegistry.Singleton().addTimerListener(keepaliveInterval, this);
    }

    public void performTimeAction(TimerEvent evt) {
        if (connection != null) {
            keepaliveCount--;
            if (reconnector.isDebug())
                System.out.println(toString() + ": decrementing keepalive counter to " + keepaliveCount);
            if (keepaliveCount <= 0) {
                if (reconnector.isDebug())
                    System.out.println(toString() + ": keepalive counter reaches 0, invalidating connection!");
                if (reconnector.isEnabled()) {
                    keepaliveCount = INITIAL_KEEPALIVE_COUNT;
                    reconnect();
                } else
                    cancelAndNotify(new Exception("Keepalive Counter reaches 0!"), true);
            } else
                performRequest(keepaliveRequest);
        }
    }

    protected void verifyState() throws JMSException {
        if (closed) {
            throw new javax.jms.IllegalStateException("Connection is closed");
        }
    }

    synchronized void increaseDuplicateLogSize(int extend) {
        duplicateLog.resize(duplicateLog.getMax() + extend);
    }

    synchronized void addToDuplicateLog(Set rollbackLog) {
        duplicateLog.addAll(rollbackLog);
    }

    synchronized boolean isDuplicate(String id) {
        boolean rc = false;
        if (duplicateLog.contains(id)) {
            rc = true;
        } else {
            duplicateLog.add(id);
        }
        return rc;
    }

    synchronized void removeFromDuplicateLog(String id) {
        duplicateLog.remove(id);
    }

    void addSession(Session session) {
        clientIdAllowed = false;
        sessionList.add(session);
        if (connectionState == CONNECTED_STARTED)
            ((SessionImpl) session).startSession();
    }

    void removeSession(Session session) {
        sessionList.remove(session);
    }

    void addConnectionConsumer(ConnectionConsumerImpl connectionConsumer) {
        connectionConsumerList.add(connectionConsumer);
        if (connectionState == CONNECTED_STARTED)
            connectionConsumer.startConsumer();
    }

    void removeConnectionConsumer(ConnectionConsumerImpl connectionConsumer) {
        connectionConsumerList.remove(connectionConsumer);
    }

    void addTmpQueue(QueueImpl queue) {
        try {
            tmpQueues.put(queue.getQueueName(), queue);
        } catch (JMSException e) {
        }
    }

    void removeTmpQueue(String queueName) {
        tmpQueues.remove(queueName);
    }

    public int getConnectionState() {
        return connectionState;
    }

    public void deleteTempQueue(String queueName) throws JMSException {
        Reply reply = null;
        try {
            reply = requestRegistry.request(new DeleteTmpQueueRequest(0, queueName));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (!reply.isOk()) {
            throw ExceptionConverter.convert(reply.getException());
        }
        tmpQueues.remove(queueName);
    }

    // --> JMS 1.1
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        verifyState();

        SessionImpl session = null;
        CreateSessionReply reply = null;

        try {
            reply =
                    (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, transacted,
                            acknowledgeMode, CreateSessionRequest.UNIFIED, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();
            String cid = clientID != null ? clientID : internalCID;

            session = new SessionImpl(SessionImpl.TYPE_SESSION, this, transacted, acknowledgeMode,
                    dispatchId, requestRegistry,
                    myHostname, cid);
            session.setUserName(getUserName());
            session.setMyDispatchId(addRequestService(session));
            addSession(session);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (session);
    }

    public ConnectionConsumer createConnectionConsumer(Queue queue,
                                                       String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException {
        verifyState();

        QueueConnectionConsumerImpl cc = null;
        CreateSessionReply reply = null;

        try {
            reply = (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, false,
                    0, CreateSessionRequest.QUEUE_SESSION, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();

            cc = new QueueConnectionConsumerImpl(this, dispatchId, requestRegistry, sessionPool, maxMessages);
            cc.setMyDispatchId(addRequestService(cc));
            String ms = messageSelector;
            if (messageSelector != null && messageSelector.trim().length() == 0)
                ms = null;
            cc.createConsumer((QueueImpl) queue, ms);
            addConnectionConsumer(cc);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (cc);
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
            throws JMSException {
        verifyState();

        TopicConnectionConsumerImpl cc = null;
        CreateSessionReply reply = null;

        try {
            reply = (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, false,
                    0, CreateSessionRequest.TOPIC_SESSION, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();

            cc = new TopicConnectionConsumerImpl(this, dispatchId, requestRegistry, sessionPool, maxMessages);
            cc.setMyDispatchId(addRequestService(cc));
            String ms = messageSelector;
            if (messageSelector != null && messageSelector.trim().length() == 0)
                ms = null;
            cc.createSubscriber((TopicImpl) topic, ms);
            addConnectionConsumer(cc);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (cc);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
            throws JMSException {
        verifyState();

        TopicConnectionConsumerImpl cc = null;
        CreateSessionReply reply = null;

        try {
            reply = (CreateSessionReply) requestRegistry.request(new CreateSessionRequest(0, false,
                    0, CreateSessionRequest.TOPIC_SESSION, 0));
        } catch (Exception e) {
            throw ExceptionConverter.convert(e);
        }

        if (reply.isOk()) {
            int dispatchId = reply.getSessionDispatchId();

            cc = new TopicConnectionConsumerImpl(this, dispatchId, requestRegistry, sessionPool, maxMessages);
            cc.setMyDispatchId(addRequestService(cc));
            cc.createDurableSubscriber((TopicImpl) topic, messageSelector, subscriptionName);
            addConnectionConsumer(cc);
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }

        return (cc);
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException {
        if (destination == null)
            throw new InvalidDestinationException("createConnectionConsumer, destination is null!");
        DestinationImpl destImpl = (DestinationImpl) destination;
        ConnectionConsumer consumer = null;
        switch (destImpl.getType()) {
            case DestinationFactory.TYPE_QUEUE:
            case DestinationFactory.TYPE_TEMPQUEUE:
                consumer = createConnectionConsumer((Queue) destination, messageSelector, sessionPool, maxMessages);
                break;
            case DestinationFactory.TYPE_TOPIC:
            case DestinationFactory.TYPE_TEMPTOPIC:
                consumer = createConnectionConsumer((Topic) destination, messageSelector, sessionPool, maxMessages);
                break;
        }
        return consumer;
    }

    // <-- JMS 1.1
    public String getClientID() throws JMSException {
        verifyState();

        return (originalCID);
    }

    public void setClientID(String s) throws JMSException {
        verifyState();

        if (clientIdAdministratively)
            throw new javax.jms.IllegalStateException("Client ID was set administratively and cannot be changed");
        if (!clientIdAllowed)
            throw new javax.jms.IllegalStateException("setClientID is only allowed immediatly after connection creation");

        clientIdAllowed = false;

        try {
            SwiftUtilities.verifyClientId(s);
        } catch (Exception e) {
            throw new InvalidClientIDException(e.getMessage());
        }

        SetClientIdReply reply = null;
        try {
            reply = (SetClientIdReply) requestRegistry.request(new SetClientIdRequest(0, s));
        } catch (Exception e) {
            throw new JMSException(e.getMessage());
        }
        if (reply.isOk()) {
            clientID = reply.getClientId();
        } else {
            throw ExceptionConverter.convert(reply.getException());
        }
        originalCID = s;
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        verifyState();

        return (metaData);
    }

    public void setExceptionListener(ExceptionListener listener)
            throws JMSException {
        // setting to null must be possible
        if (listener != null)
            verifyState();
        exceptionListener = listener;
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        verifyState();
        return exceptionListener;
    }

    private void writeObject(Dumpable obj) throws Exception {
        Dumpalizer.dump(outStream, obj);
        outStream.flush();
    }

    public void performRequest(Request request) {
        int id = request.getConnectionId();
        RequestRetryValidator validator = request.getValidator();
        if (id != -1 && id != connectionId && validator != null) {
            try {
                validator.validate(request);
            } catch (ValidationException e) {
                return;
            }
        }
        connectionQueue.enqueue(request);
    }

    public void performReply(Reply reply) {
        connectionQueue.enqueue(reply);
    }

    public void onException(IOException exception) {
        if (closed) {
            return;
        }
        if (reconnector.isEnabled()) {
            reconnect();
        } else
            cancelAndNotify(exception, true);
    }

    private void dispatchDumpable(Dumpable obj) {
        if (obj.getDumpId() != SMQPFactory.DID_KEEPALIVE_REQ) {
            if (obj instanceof Reply) {
                requestRegistry.setReply((Reply) obj);
            } else if (obj instanceof Request) {
                Request req = (Request) obj;
                req.setConnectionId(connectionId);
                dispatch(req);
            } else {
                // unknown class
            }
        } else {
            keepaliveCount = 5;
            if (reconnector.isDebug())
                System.out.println(toString() + ": setting keepalive counter to " + keepaliveCount);
        }
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        try {
            Dumpable obj = Dumpalizer.construct(in, dumpableFactory);
            if (obj == null) {
                return;
            }
            if (obj.getDumpId() == SMQPFactory.DID_BULK_REQ) {
                SMQPBulkRequest bulkRequest = (SMQPBulkRequest) obj;
                for (int i = 0; i < bulkRequest.len; i++) {
                    if (connection == null)
                        return;
                    dispatchDumpable((Dumpable) bulkRequest.dumpables[i]);
                }
            } else {
                if (connection == null)
                    return;
                dispatchDumpable(obj);
            }
        } catch (Exception e) {
            if (closed) {
                return;
            }
            if (reconnector.isEnabled()) {
                reconnect();
            } else
                cancelAndNotify(e, true);
        }
    }

    public synchronized void start() throws JMSException {
        verifyState();
        clientIdAllowed = false;

        if (connectionState == CONNECTED_STOPPED) {
            for (int i = 0; i < sessionList.size(); i++) {
                ((SessionImpl) sessionList.get(i)).startSession();
            }
            for (int i = 0; i < connectionConsumerList.size(); i++) {
                ((ConnectionConsumerImpl) connectionConsumerList.get(i)).startConsumer();
            }
            connectionState = CONNECTED_STARTED;
        } else if (connectionState == DISCONNECTED) {
            throw new IllegalStateException("could not start - connection is disconnected!");
        }
    }

    public void stop() throws JMSException {
        verifyState();
        clientIdAllowed = false;

        if (connectionState == CONNECTED_STARTED) {
            for (int i = 0; i < sessionList.size(); i++) {
                ((SessionImpl) sessionList.get(i)).stopSession();
            }
            for (int i = 0; i < connectionConsumerList.size(); i++) {
                ((ConnectionConsumerImpl) connectionConsumerList.get(i)).stopConsumer();
            }
            connectionState = CONNECTED_STOPPED;
        } else if (connectionState == DISCONNECTED) {
            throw new IllegalStateException("could not stop - connection is disconnected!");
        }
    }

    public void close() throws JMSException {
        if (closed)
            return;

        if (connectionState == DISCONNECTED) {
            throw new javax.jms.IllegalStateException("could not close - connection is disconnected!");
        }

        try {
            TimerRegistry.Singleton().removeTimerListener(keepaliveInterval, this);

            setExceptionListener(null);
            SessionImpl[] si = (SessionImpl[]) sessionList.toArray(new SessionImpl[sessionList.size()]);
            for (int i = 0; i < si.length; i++) {
                SessionImpl session = (SessionImpl) si[i];
                if (!session.isClosed())
                    session.close();
            }
            ConnectionConsumerImpl[] ci = (ConnectionConsumerImpl[]) connectionConsumerList.toArray(new ConnectionConsumerImpl[connectionConsumerList.size()]);
            for (int i = 0; i < ci.length; i++) {
                ConnectionConsumerImpl cc = (ConnectionConsumerImpl) ci[i];
                if (!cc.isClosed())
                    cc.close();
            }

            requestRegistry.request(new DisconnectRequest());
            connector.close();

            closed = true;
            connectionQueue.stopQueue();
            reconnector.invalidateConnection();
            requestRegistry.cancelAllRequests(new TransportException("Connection closed"), false);
            requestRegistry.close();
            sessionList.clear();
            tmpQueues.clear();
            duplicateLog.clear();
            connectionState = DISCONNECTED;
        } catch (Exception e) {
            throw new JMSException(e.getMessage());
        }
    }

    public void cancel(boolean closeReconnector) {
        if (!cancelled) {
            if (closeReconnector)
                connector.close();
            connectionQueue.stopQueue();
            cancelled = true;
            closed = true;
            for (int i = 0; i < sessionList.size(); i++) {
                SessionImpl session = (SessionImpl) sessionList.get(i);
                session.cancel();
            }
            sessionList.clear();
            for (int i = 0; i < connectionConsumerList.size(); i++) {
                ConnectionConsumerImpl cc = (ConnectionConsumerImpl) connectionConsumerList.get(i);
                cc.cancel();
            }
            connectionConsumerList.clear();
            TimerRegistry.Singleton().removeTimerListener(keepaliveInterval, this);
            reconnector.invalidateConnection();
        }
        requestRegistry.cancelAllRequests(new TransportException("Connection closed"), false);
        requestRegistry.close();
        tmpQueues.clear();
        duplicateLog.clear();
        connectionState = DISCONNECTED;
    }

    public void cancelAndNotify(Exception exception, boolean closeReconnector) {
        cancel(closeReconnector);

        if (exceptionListener != null) {
            exceptionListener.onException(new ConnectionLostException(exception.getMessage()));
            exceptionListener = null;
        }
    }

    private class ConnectionQueue extends SingleProcessorQueue {
        SMQPBulkRequest bulkRequest = new SMQPBulkRequest();

        public ConnectionQueue() {
            super(100);
        }

        protected void startProcessor() {
            if (!closed)
                connectionPool.dispatchTask(connectionTask);
        }

        protected void process(Object[] bulk, int n) {
            try {
                if (n == 1)
                    writeObject((Dumpable) bulk[0]);
                else {
                    bulkRequest.dumpables = bulk;
                    bulkRequest.len = n;
                    writeObject(bulkRequest);
                }
            } catch (Exception e) {
                if (reconnector.isEnabled()) {
                    reconnect();
                } else
                    cancelAndNotify(e, true);
            }
        }
    }

    private class ConnectionTask implements AsyncTask {
        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return DISPATCH_TOKEN;
        }

        public String getDescription() {
            return myHostname + "/Connection/ConnectionTask";
        }

        public void run() {
            if (!closed && connectionQueue.dequeue())
                connectionPool.dispatchTask(this);
        }

        public void stop() {
        }
    }
}



