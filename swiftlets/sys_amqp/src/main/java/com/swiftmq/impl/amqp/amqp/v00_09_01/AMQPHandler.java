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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.amqp.v091.generated.Constants;
import com.swiftmq.amqp.v091.generated.channel.ChannelMethod;
import com.swiftmq.amqp.v091.generated.channel.ChannelMethodVisitor;
import com.swiftmq.amqp.v091.generated.channel.Flow;
import com.swiftmq.amqp.v091.generated.channel.FlowOk;
import com.swiftmq.amqp.v091.generated.connection.*;
import com.swiftmq.amqp.v091.types.*;
import com.swiftmq.impl.amqp.Handler;
import com.swiftmq.impl.amqp.OutboundTracer;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;
import com.swiftmq.impl.amqp.accounting.AccountingProfile;
import com.swiftmq.impl.amqp.amqp.v00_09_01.po.*;
import com.swiftmq.impl.amqp.sasl.provider.AnonServer;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.LengthCaptureDataInput;
import com.swiftmq.util.Version;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AMQPHandler implements Handler, AMQPConnectionVisitor {
    static final Frame HEARTBEATFRAME = new Frame(Frame.TYPE_HEARTBEAT, 0, 0, null);
    static final String VERSION = "0.9.1";
    SwiftletContext ctx = null;
    VersionedConnection versionedConnection = null;
    Entity connectionTemplate = null;
    PipelineQueue pipelineQueue = null;
    boolean closed = false;
    boolean closeInProgress = false;
    boolean closeFrameSent = false;
    Lock closeLock = new ReentrantLock();
    String hostname = null;
    boolean connectionDisabled = false;
    Entity usage = null;
    Property receivedSecProp = null;
    Property sentSecProp = null;
    Property receivedTotalProp = null;
    Property sentTotalProp = null;
    Property authEnabled = null;
    int maxLocalFrameSize = 0;
    int maxRemoteFrameSize = 0;
    SaslServer saslServer = null;
    String saslMechanisms = null;
    String userName = "anonymous";
    String realm = null;
    ActiveLogin activeLogin = null;
    ConnectionVisitor connectionVisitor = new ConnectionVisitor();
    ChannelDispatchVisitor channelDispatchVisitor = new ChannelDispatchVisitor();
    List channels = new ArrayList();
    long myIdleTimeout = 0;
    long theirHeartbeatInterval = 0;
    long lastActivity = 0;
    TimerListener heartBeatSender = null;
    TimerListener idleTimeoutChecker = null;
    List<String> tempQueues = new ArrayList<String>();

    public AMQPHandler(SwiftletContext ctx, VersionedConnection versionedConnection) {
        this.ctx = ctx;
        this.versionedConnection = versionedConnection;
        versionedConnection.setOutboundTracer(new OutboundTracer() {
            public String getTraceKey() {
                return "amqp-091";
            }

            public String getTraceString(Object obj) {
                if (obj instanceof Frame) {
                    try {
                        ((Frame) obj).generatePayloadObject();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "[" + ((Frame) obj).getChannel() + "] " + obj;
                } else
                    return obj.toString();
            }
        });
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
        }
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(VersionedConnection.TP_CONNECTIONSVC), "AMQPHandler", this);
        connectionTemplate = versionedConnection.getConnectionTemplate();
        maxLocalFrameSize = ((Long) connectionTemplate.getProperty("max-frame-size").getValue()).intValue();
        usage = versionedConnection.getUsage();
        receivedSecProp = usage.getProperty("msgs-received");
        sentSecProp = usage.getProperty("msgs-sent");
        receivedTotalProp = usage.getProperty("total-received");
        sentTotalProp = usage.getProperty("total-sent");
        authEnabled = SwiftletManager.getInstance().getConfiguration("sys$authentication").getProperty("authentication-enabled");
        myIdleTimeout = ((Long) connectionTemplate.getProperty("idle-timeout").getValue()).longValue();
        dispatch(new POSendStart());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", created");
    }

    public void toPayload(Frame frame, Method method) {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        try {
            method.writeContent(dbos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        frame.setPayload(dbos.getBuffer());
        frame.setSize(dbos.getCount());
    }

    public void toPayload(Frame frame, ContentHeaderProperties contentHeaderProperties) {
        DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
        try {
            contentHeaderProperties.writeContent(dbos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        frame.setPayload(dbos.getBuffer());
        frame.setSize(dbos.getCount());
    }

    public int getMaxFrameSize() {
        // smallest size of local/remote size but at least 512
        return Math.max(512, Math.min(maxLocalFrameSize, maxRemoteFrameSize));
    }

    public synchronized void addTempQueue(String name) {
        tempQueues.add(name);
    }

    public synchronized void removeTempQueue(String name) {
        tempQueues.remove(name);
    }

    public VersionedConnection getVersionedConnection() {
        return versionedConnection;
    }

    private String createSaslMechanisms() {
        StringBuffer b = new StringBuffer();
        for (Enumeration _enum = Sasl.getSaslServerFactories(); _enum.hasMoreElements(); ) {
            SaslServerFactory sf = (SaslServerFactory) _enum.nextElement();
            String[] mnames = sf.getMechanismNames(null);
            if (mnames != null) {
                for (int i = 0; i < mnames.length; i++) {
                    if (!mnames[i].toUpperCase().equals("GSSAPI")) {
                        if (mnames[i].endsWith(AnonServer.MECHNAME)) {
                            if (!((Boolean) authEnabled.getValue()).booleanValue()) {
                                if (b.length() > 0)
                                    b.append(" ");
                                b.append(mnames[i]);
                            }
                        } else {
                            if (b.length() > 0)
                                b.append(" ");
                            b.append(mnames[i]);
                        }
                    }
                }
            }
        }
        return b.toString();
    }

    private boolean hasMechanism(String mechanism) {
        StringTokenizer t = new StringTokenizer(saslMechanisms, " ");
        while (t.hasMoreTokens())
            if (t.nextToken().equals(mechanism))
                return true;
        return false;
    }

    private void processResponse(byte[] response) throws SaslException {
        byte[] challenge = saslServer.evaluateResponse(response);
        if (saslServer.isComplete()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", processResponse, complete, userName: " + userName + ", realm: " + realm);
            activeLogin = ctx.authSwiftlet.createActiveLogin(userName, "AMQP");
            dispatch(new POSendTune());
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", processResponse, not complete");
            Secure secure = new Secure();
            secure.setChallenge(challenge);
            Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
            toPayload(frame, secure);
            versionedConnection.send(frame);
        }
    }

    public ActiveLogin getActiveLogin() {
        return activeLogin;
    }

    public String getVersion() {
        return VERSION;
    }

    public void collect(long lastCollect) {
    }

    public void startAccounting(AccountingProfile accountingProfile) {
    }

    public void stopAccounting() {
    }

    public void flushAccounting() {
    }

    public void dispatch(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void visit(POSendStart po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        Start start = new Start();
        start.setVersionMajor((byte) 0);
        start.setVersionMinor((byte) 9);
        try {
            saslMechanisms = createSaslMechanisms();
            start.setMechanisms(saslMechanisms.getBytes("utf-8"));
            start.setLocales("en_US".getBytes("utf-8"));
            Map serverProps = new HashMap();
            serverProps.put("product", new Field('S', "SwiftMQ".getBytes("utf-8")));
            serverProps.put("release", new Field('S', Version.getKernelVendor().getBytes("utf-8")));
            serverProps.put("vendor", new Field('S', Version.getKernelVendor().getBytes("utf-8")));
            Map capas = new HashMap();
            capas.put("exchange_exchange_bindings", new Field('t', new Boolean(false)));
            capas.put("consumer_cancel_notify", new Field('t', new Boolean(false)));
            capas.put("basic.nack", new Field('t', new Boolean(false)));
            capas.put("publisher_confirms", new Field('t', new Boolean(false)));
            serverProps.put("capabilities", new Field('F', capas));
            start.setServerProperties(serverProps);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
        toPayload(frame, start);
        versionedConnection.send(frame);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendTune po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        lastActivity = System.currentTimeMillis();
        Tune tune = new Tune();
        int channelMax = ((Integer) connectionTemplate.getProperty("max-channel-number").getValue()).intValue();
        tune.setChannelMax(channelMax > Short.MAX_VALUE ? 0 : channelMax);
        tune.setFrameMax(maxLocalFrameSize);
        tune.setHeartbeat((int) (myIdleTimeout / 1000));
        Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
        toPayload(frame, tune);
        versionedConnection.send(frame);
        if (myIdleTimeout > 0) {
            idleTimeoutChecker = new TimerListener() {
                public void performTimeAction() {
                    dispatch(new POCheckIdleTimeout(null));
                }
            };
            ctx.timerSwiftlet.addTimerListener(myIdleTimeout + 5000, idleTimeoutChecker);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendHeartBeat po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        versionedConnection.send(HEARTBEATFRAME);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POCheckIdleTimeout po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        long to = lastActivity + myIdleTimeout;
        if (System.currentTimeMillis() > to) {
            ctx.logSwiftlet.logWarning(ctx.amqpSwiftlet.getName(), toString() + ", idleTimeout reached (" + myIdleTimeout + " ms). Closing connection!");
            dispatch(new POSendClose(Constants.CONNECTION_FORCED, "IdleTimeout reached (" + myIdleTimeout + " ms)"));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendChannelClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        if (channels.size() >= po.getChannelNo()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " channel seems to be closed already");
            return;
        }
        ChannelHandler channelHandler = (ChannelHandler) channels.remove(po.getChannelNo());
        if (channelHandler != null)
            channelHandler.close();
        com.swiftmq.amqp.v091.generated.channel.Close close = new com.swiftmq.amqp.v091.generated.channel.Close();
        if (po.getErrorCondition() != -1) {
            close.setReplyCode(po.getErrorCondition());
            if (po.getDescription() != null)
                close.setReplyText(po.getDescription());
            else
                close.setReplyText("");
            if (po.getFailedMethod() != null) {
                close.setClassId(po.getFailedMethod()._getClassId());
                close.setMethodId(po.getFailedMethod()._getMethodId());
            }
        } else {
            close.setReplyCode(Constants.REPLY_SUCCESS);
            close.setReplyText("OK");
        }
        Frame frame = new Frame(Frame.TYPE_METHOD, po.getChannelNo(), 0, null);
        toPayload(frame, close);
        versionedConnection.send(frame);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        Close close = new Close();
        if (po.getErrorCondition() != -1) {
            close.setReplyCode(po.getErrorCondition());
            close.setReplyText(po.getDescription());
            connectionDisabled = true;
        } else {
            close.setReplyCode(Constants.REPLY_SUCCESS);
            close.setReplyText("OK");
        }
        Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
        toPayload(frame, close);
        frame.setCallback(new AsyncCompletionCallback() {
            public void done(boolean b) {
                ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
            }
        });
        versionedConnection.send(frame);
        closeFrameSent = true;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        if (heartBeatSender != null) {
            ctx.timerSwiftlet.removeTimerListener(heartBeatSender);
            heartBeatSender = null;
        }
        if (idleTimeoutChecker != null) {
            ctx.timerSwiftlet.removeTimerListener(idleTimeoutChecker);
            idleTimeoutChecker = null;
        }
        for (int i = 0; i < channels.size(); i++) {
            ChannelHandler channelHandler = (ChannelHandler) channels.get(i);
            if (channelHandler != null)
                channelHandler.close();
        }
        channels.clear();
        for (int i = 0; i < tempQueues.size(); i++) {
            try {
                String name = tempQueues.get(i);
                ctx.queueMapper.unmapTempQueue(name);
                ctx.queueManager.deleteTemporaryQueue(name);
            } catch (QueueException e) {
            }
        }
        tempQueues.clear();
        if (activeLogin != null)
            ctx.removeId(activeLogin.getClientId());
        closed = true;
        pipelineQueue.close();
        po.setSuccess(true);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close ...");
        closeLock.lock();
        if (closeInProgress) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close in progress, return");
            return;
        }
        closeInProgress = true;
        closeLock.unlock();
        Semaphore sem = new Semaphore();
        dispatch(new POClose(sem));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close done");
    }

    public void dataAvailable(LengthCaptureDataInput lengthCaptureDataInput) {
        try {
            lastActivity = System.currentTimeMillis();
            Frame frame = new Frame(maxLocalFrameSize);
            frame.readContent(lengthCaptureDataInput);
            if (ctx.protSpace.enabled)
                ctx.protSpace.trace("amqp-091", versionedConnection.toString() + "/RCV: [" + frame.getChannel() + "] " + frame.generatePayloadObject());
            switch (frame.getType()) {
                case Frame.TYPE_METHOD:
                    if (frame.getChannel() == 0)
                        ((ConnectionMethod) frame.getPayloadObject()).accept(connectionVisitor);
                    else
                        channelDispatchVisitor.dispatchChannel(frame);
                    break;
                case Frame.TYPE_HEADER:
                case Frame.TYPE_BODY:
                    channelDispatchVisitor.dispatchChannel(frame);
                    break;
                case Frame.TYPE_HEARTBEAT:
                    break;
                default:
                    throw new Exception("Invalid frame type received: " + frame.getType());
            }
        } catch (FrameSizeExceededException fxe) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", " + fxe.getMessage());
            ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), "Connection closed. " + fxe.getMessage());
            dispatch(new POSendClose(Constants.CONTENT_TOO_LARGE, fxe.getMessage()));
            connectionDisabled = true;
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            dispatch(new POSendClose(Constants.FRAME_ERROR, e.toString()));
            connectionDisabled = true;
        }
    }

    public String toString() {
        return versionedConnection.toString() + "/AMQPHandler/" + VERSION;
    }

    private class ConnectionVisitor implements ConnectionMethodVisitor {
        public void visit(Start start) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Start ...");
            dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Invalid method received: " + start));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Start done");
        }

        public void visit(StartOk startOk) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit StartOk ...");
            String mechanism = startOk.getMechanism();
            if (!hasMechanism(mechanism))
                dispatch(new POSendClose(Constants.NOT_IMPLEMENTED, "Invalid SASL mechanism: " + mechanism));
            else {
                try {
                    saslServer = Sasl.createSaslServer(mechanism, "AMQP", SwiftletManager.getInstance().getRouterName(), null, new CallbackHandlerImpl());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit StartOK, saslServer: " + saslServer);
                    processResponse(startOk.getResponse() != null ? startOk.getResponse() : new byte[0]);
                } catch (SaslException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", StartOK, SASL exception: " + e);
                    dispatch(new POSendClose(Constants.ACCESS_REFUSED, "SASL exception: " + e));
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit StartOk done");
        }

        public void visit(Secure secure) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Secure ...");
            dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Invalid method received: " + secure));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Secure done");
        }

        public void visit(SecureOk secureOk) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SecureOk ...");
            try {
                processResponse(secureOk.getResponse());
            } catch (SaslException e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", StartOK, SASL exception: " + e);
                dispatch(new POSendClose(Constants.ACCESS_REFUSED, "SASL exception: " + e));
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit SecureOk done");
        }

        public void visit(Tune tune) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Tune ...");
            dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Invalid method received: " + tune));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Tune done");
        }

        public void visit(TuneOk tuneOk) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit TuneOk ...");
            if (activeLogin == null) {
                dispatch(new POSendClose(Constants.ACCESS_REFUSED, "Not authenticated!"));
                return;
            }
            theirHeartbeatInterval = tuneOk.getHeartbeat() * 1000;
            if (theirHeartbeatInterval > 0) {
                heartBeatSender = new TimerListener() {
                    public void performTimeAction() {
                        dispatch(new POSendHeartBeat(null));
                    }
                };
                ctx.timerSwiftlet.addTimerListener(theirHeartbeatInterval, heartBeatSender);
            }
            maxRemoteFrameSize = tuneOk.getFrameMax();
            if (maxRemoteFrameSize == 0)
                maxRemoteFrameSize = Integer.MAX_VALUE;

            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit TuneOk done");
        }

        public void visit(Open open) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Open ...");
            if (activeLogin == null) {
                dispatch(new POSendClose(Constants.ACCESS_REFUSED, "Not authenticated!"));
                return;
            }
            Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
            OpenOk openOk = new OpenOk();
            openOk.setReserved1("");
            toPayload(frame, openOk);
            versionedConnection.send(frame);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Open done");
        }

        public void visit(OpenOk openOk) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit OpenOk ...");
            dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Invalid method received: " + openOk));
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit OpenOk done");
        }

        public void visit(Close close) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Close ...");
            if (activeLogin == null) {
                dispatch(new POSendClose(Constants.ACCESS_REFUSED, "Not authenticated!"));
                return;
            }
            Frame frame = new Frame(Frame.TYPE_METHOD, 0, 0, null);
            toPayload(frame, new CloseOk());
            versionedConnection.send(frame);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit Close done");
        }

        public void visit(CloseOk closeOk) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit CloseOk ...");
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit CloseOk done");
        }

        public String toString() {
            return AMQPHandler.this.toString() + "/ConnectionVisitor";
        }
    }

    private class ChannelDispatchVisitor implements ChannelMethodVisitor {
        ChannelHandler channelHandler = null;
        int channelNo = -1;

        public void dispatchChannel(Frame frame) {
            if (activeLogin == null) {
                dispatch(new POSendClose(Constants.ACCESS_REFUSED, "Not authenticated!"));
                return;
            }
            channelNo = frame.getChannel();
            if (channelNo < 0) {
                dispatch(new POSendClose(Constants.INTERNAL_ERROR, "Channel no is invalid! (" + channelNo + ")"));
                return;
            }
            if (channelNo >= 0 && channelNo < channels.size())
                channelHandler = (ChannelHandler) channels.get(channelNo);
            try {
                if (frame.getType() == Frame.TYPE_METHOD) {
                    Method method = (Method) frame.getPayloadObject();
                    if (method._getClassId() == 20 && method._getMethodId() != 20)  // Not Flow
                        ((ChannelMethod) method).accept(this);
                    else if (channelHandler != null)
                        channelHandler.dispatch(new POChannelFrameReceived(frame));
                    else
                        dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Channel " + frame.getChannel() + " unknown"));
                } else if (channelHandler != null)
                    channelHandler.dispatch(new POChannelFrameReceived(frame));
                else
                    dispatch(new POSendClose(Constants.UNEXPECTED_FRAME, "Channel " + frame.getChannel() + " unknown"));
            } catch (IOException e) {
                dispatch(new POSendClose(Constants.FRAME_ERROR, e.toString()));
            } finally {
                channelHandler = null;
                channelNo = -1;
            }
        }

        private List ensureSize(int size, List list) {
            if (size < list.size())
                return list;
            for (int i = list.size(); i <= size; i++)
                list.add(null);
            return list;
        }

        public void visit(com.swiftmq.amqp.v091.generated.channel.Open open) {
            if (channelHandler != null) {
                dispatch(new POSendClose(Constants.CHANNEL_ERROR, "Channel " + channelNo + " is already active."));
                return;
            }
            ChannelHandler channelHandler = new ChannelHandler(ctx, AMQPHandler.this, channelNo);
            ensureSize(channelNo, channels).set(channelNo, channelHandler);
            com.swiftmq.amqp.v091.generated.channel.OpenOk openOk = new com.swiftmq.amqp.v091.generated.channel.OpenOk();
            openOk.setReserved1(new byte[0]);
            Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
            toPayload(frame, openOk);
            versionedConnection.send(frame);
        }

        public void visit(com.swiftmq.amqp.v091.generated.channel.OpenOk openOk) {
            dispatch(new POSendClose(Constants.CHANNEL_ERROR, "Invalid method received: " + openOk));
        }

        public void visit(Flow flow) {
        }

        public void visit(FlowOk flowOk) {
        }

        public void visit(com.swiftmq.amqp.v091.generated.channel.Close close) {
            if (channelHandler == null) {
                dispatch(new POSendClose(Constants.CHANNEL_ERROR, "Channel " + channelNo + " does not exist."));
                return;
            }
            channels.set(channelNo, null);
            channelHandler.close();
            com.swiftmq.amqp.v091.generated.channel.CloseOk closeOk = new com.swiftmq.amqp.v091.generated.channel.CloseOk();
            Frame frame = new Frame(Frame.TYPE_METHOD, channelNo, 0, null);
            toPayload(frame, closeOk);
            versionedConnection.send(frame);
        }

        public void visit(com.swiftmq.amqp.v091.generated.channel.CloseOk closeOk) {
            dispatch(new POSendClose(Constants.CHANNEL_ERROR, "Invalid method received: " + closeOk));
        }
    }

    private class CallbackHandlerImpl implements CallbackHandler {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle ...");
            PasswordCallback pwc = null;
            AuthorizeCallback azc = null;
            for (int i = 0; i < callbacks.length; i++) {
                Callback c = callbacks[i];
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, c=" + c);
                if (c instanceof NameCallback)
                    userName = ((NameCallback) c).getDefaultName();
                else if (c instanceof PasswordCallback)
                    pwc = (PasswordCallback) c;
                else if (c instanceof RealmCallback)
                    realm = ((RealmCallback) c).getDefaultText();
                else if (c instanceof AuthorizeCallback)
                    azc = (AuthorizeCallback) c;
                else
                    throw new UnsupportedEncodingException(c.getClass().getName());
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, userName=" + userName);
            if (userName != null) {
                try {
                    String password = ctx.authSwiftlet.getPassword(userName);
                    if (pwc != null)
                        pwc.setPassword(password.toCharArray());
                    if (azc != null) {
                        azc.setAuthorized(true);
                        azc.setAuthorizedID(userName);
                    }
                } catch (com.swiftmq.swiftlet.auth.AuthenticationException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle, exception=" + e);
                    if (azc != null)
                        azc.setAuthorized(false);
                }
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", CallbackHandlerImpl.handle done");
        }
    }
}
