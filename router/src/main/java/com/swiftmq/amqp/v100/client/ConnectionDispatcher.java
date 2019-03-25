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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.OutboundHandler;
import com.swiftmq.amqp.ProtocolHeader;
import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.amqp.v100.client.po.*;
import com.swiftmq.amqp.v100.generated.FrameReader;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.ConnectionError;
import com.swiftmq.amqp.v100.generated.transport.definitions.ErrorConditionFactory;
import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.transport.HeartbeatFrame;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.net.client.InboundHandler;
import com.swiftmq.net.protocol.amqp.AMQPInputHandler;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.timer.TimerEvent;
import com.swiftmq.tools.timer.TimerListener;
import com.swiftmq.tools.timer.TimerRegistry;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import javax.security.auth.callback.*;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionDispatcher
        implements ConnectionVisitor, InboundHandler {
    static final HeartbeatFrame HEARTBEAT_FRAME = new HeartbeatFrame(0);

    AMQPContext ctx = null;
    Tracer fTracer = null;
    Tracer pTracer = null;
    com.swiftmq.amqp.v100.client.Connection myConnection = null;
    String remoteHostname = null;
    String localHostname = null;
    OutboundHandler outboundHandler = null;
    PipelineQueue pipelineQueue = null;
    DispatchVisitor dispatchVisitor = new DispatchVisitor();
    ConnectionVisitor connectionVisitor = new ConnectionVisitor();
    AMQPInputHandler protocolHandler = new AMQPInputHandler();
    boolean closed = false;
    boolean closeInProgress = false;
    Lock closeLock = new ReentrantLock();
    boolean awaitProtocolHeader = true;
    ProtocolHeader localProt = null;
    ProtocolHeader remoteProt = null;
    POProtocolRequest protPO = null;
    POAuthenticate authPO = null;
    POOpen openPO = null;
    OpenFrame remoteOpen = null;
    POSendClose closePO = null;
    CloseFrame remoteClose = null;
    SaslMechanismsFrame saslMechanisms = null;
    boolean saslActive = false;
    volatile long lastActivity = System.currentTimeMillis();
    long myIdleTimeout = -1;
    TimerListener heartBeatSender = null;
    long heartBeatDelay = 0;
    TimerListener idleTimeoutChecker = null;
    long idleTimeoutDelay = 0;
    SaslClient saslClient = null;
    boolean connectionDisabled = false;

    int maxLocalFrameSize = Integer.MAX_VALUE;
    int maxRemoteFrameSize = Integer.MAX_VALUE;

    public ConnectionDispatcher(AMQPContext ctx, String remoteHostname) {
        this.ctx = ctx;
        this.remoteHostname = remoteHostname;
        fTracer = ctx.getFrameTracer();
        pTracer = ctx.getProcessingTracer();
        try {
            localHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            localHostname = "unknown";
        }
        pipelineQueue = new PipelineQueue(ctx.getConnectionPool(), "ConnectionDispatcher", this);
    }

    public AMQPInputHandler getProtocolHandler() {
        return protocolHandler;
    }

    public void setMyConnection(Connection myConnection) {
        this.myConnection = myConnection;
    }

    public void setOutboundHandler(OutboundHandler outboundHandler) {
        this.outboundHandler = outboundHandler;
    }

    public int getMaxFrameSize() {
        // smallest size of local/remote size but at least 512
        return Math.max(512, Math.min(maxLocalFrameSize, maxRemoteFrameSize));
    }

    public void setSaslActive(boolean saslActive) {
        this.saslActive = saslActive;
    }

    private void checkCompatibility() {
        if (localProt != null && remoteProt != null) {
            if (!localProt.equals(remoteProt)) {
                protPO.setSuccess(false);
                protPO.setException("Incompatible AMQP protocols. Local=" + localProt + ", remote=" + remoteProt);
            } else
                protPO.setSuccess(true);
            localProt = null;
            remoteProt = null;
            if (protPO != null)
                protPO.getSemaphore().notifySingleWaiter();
            protPO = null;
        }
    }

    private static boolean hasValue(AMQPType[] t, String value)
            throws IOException {
        for (int i = 0; i < t.length; i++) {
            if (((AMQPSymbol) t[i]).getValue().equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private void checkStartSaslInit() {
        if (authPO != null && saslMechanisms != null) {
            AMQPArray mechanisms = saslMechanisms.getSaslServerMechanisms();
            if (mechanisms != null) {
                try {
                    AMQPType[] t = mechanisms.getValue();
                    if (authPO.getUsername() != null && authPO.getPassword() != null) {
                        if (hasValue(t, authPO.getMechanism())) {
                            saslClient = Sasl.createSaslClient(new String[]{authPO.getMechanism()},
                                    null, "amqp", remoteHostname, null, new CBHandler(authPO.getUsername(), authPO.getPassword()));
                            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                            SaslInitFrame initFrame = new SaslInitFrame(0);
                            initFrame.setMechanism(new AMQPSymbol(authPO.getMechanism()));
                            initFrame.setHostname(new AMQPString(myConnection.hostname));
                            if (response != null)
                                initFrame.setInitialResponse(new AMQPBinary(response));
                            outboundHandler.send(initFrame);
                            saslMechanisms = null;
                        } else {
                            authPO.setSuccess(false);
                            authPO.setException("Server doesn't support security mechanisms '" + authPO.getMechanism() + "'");
                            authPO.getSemaphore().notifySingleWaiter();
                            authPO = null;
                        }
                        saslMechanisms = null;
                    } else {
                        if (hasValue(t, "ANONYMOUS")) {
                            SaslInitFrame initFrame = new SaslInitFrame(0);
                            initFrame.setMechanism(new AMQPSymbol("ANONYMOUS"));
                            initFrame.setHostname(new AMQPString(localHostname));
                            outboundHandler.send(initFrame);
                            saslMechanisms = null;
                        } else {
                            authPO.setSuccess(false);
                            authPO.setException("Remote server doesn't support ANONYMOUS login");
                            authPO.getSemaphore().notifySingleWaiter();
                            authPO = null;
                            saslMechanisms = null;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void checkBothSidesOpen() {
        if (openPO != null && remoteOpen != null) {
            openPO.setSuccess(true);
            openPO.getSemaphore().notifySingleWaiter();
            openPO = null;
        }
    }

    private void checkBothSidesClosed() {
        if (closePO != null && remoteClose != null) {
            closePO.setSuccess(true);
            closePO.getSemaphore().notifySingleWaiter();
            closePO = null;
            remoteClose = null;
        }
    }

    private void notifyWaitingPOs(POObject[] po) {
        for (int i = 0; i < po.length; i++) {
            if (po[i] != null) {
                po[i].setSuccess(false);
                if (po[i].getException() == null)
                    po[i].setException("Connection was asynchronously closed");
                po[i].getSemaphore().notifySingleWaiter();
            }
        }
    }

    private void dispatchSession(int remoteChannel, POObject po) {
        Session session = myConnection.getSessionForRemoteChannel(remoteChannel);
        if (session != null)
            session.dispatch(po);
        else if (pTracer.isEnabled())
            pTracer.trace(toString(), ", invalid channel (no associated session): " + remoteChannel);
    }

    public void dispatch(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        try {
            if (connectionDisabled) {
                if (fTracer.isEnabled()) fTracer.trace(toString(), "Connection is disabled, ignore inbound traffic");
                return;
            }
            lastActivity = System.currentTimeMillis();
            if (awaitProtocolHeader) {
                ProtocolHeader header = new ProtocolHeader();
                header.readContent(in);
                awaitProtocolHeader = false;
                protocolHandler.setProtHeaderExpected(false);
                if (fTracer.isEnabled()) fTracer.trace("amqp", "RCV: " + header);
                dispatch(new POProtocolResponse(header));
            } else {
                if (saslActive) {
                    SaslFrameIF frame = FrameReader.createSaslFrame(in);
                    int size = frame.getPredictedSize();
                    if (size > maxLocalFrameSize) {
                        if (fTracer.isEnabled())
                            fTracer.trace(toString(), ", dataAvailable, Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")");
                        connectionDisabled = true;
                        new Disconnecter(ConnectionError.FRAMING_ERROR.getValue(), "Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")").start();
                    } else {
                        if (fTracer.isEnabled())
                            fTracer.trace("amqp", "RCV[" + ((AMQPFrame) frame).getChannel() + "] (size=" + size + "): " + frame);
                        frame.accept(dispatchVisitor);
                    }
                } else {
                    FrameIF frame = FrameReader.createFrame(in);
                    int size = frame.getPredictedSize();
                    if (size > maxLocalFrameSize) {
                        if (fTracer.isEnabled())
                            fTracer.trace(toString(), ", dataAvailable, Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")");
                        connectionDisabled = true;
                        new Disconnecter(ConnectionError.FRAMING_ERROR.getValue(), "Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")").start();
                    } else {
                        if (fTracer.isEnabled())
                            fTracer.trace("amqp", "RCV[" + ((AMQPFrame) frame).getChannel() + "] (size=" + size + "): " + frame + (((AMQPFrame) frame).getPayload() != null ? " | payload size=" + ((AMQPFrame) frame).getPayload().length : ""));
                        frame.accept(dispatchVisitor);
                    }
                }
            }
        } catch (Exception e) {
            new Disconnecter(ConnectionError.FRAMING_ERROR.getValue(), e.toString()).start();
        }
    }

    public void visit(POProtocolRequest po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        localProt = po.getHeader();
        protPO = po;
        outboundHandler.send(localProt);
        checkCompatibility();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POProtocolResponse po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        remoteProt = po.getHeader();
        checkCompatibility();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POAuthenticate po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        authPO = po;
        checkStartSaslInit();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POOpen po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        openPO = po;
        try {
            OpenFrame openFrame = new OpenFrame(0);
            openFrame.setContainerId(new AMQPString(po.getContainerId()));
            openFrame.setChannelMax(new AMQPUnsignedShort(po.getMaxChannel()));
            if (myConnection.getOpenHostname() == null)
                openFrame.setHostname(new AMQPString(remoteHostname));
            else
                openFrame.setHostname(new AMQPString(myConnection.getOpenHostname()));
            maxLocalFrameSize = po.getMaxFrameSize() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) po.getMaxFrameSize();
            openFrame.setMaxFrameSize(new AMQPUnsignedInt(maxLocalFrameSize));
            myIdleTimeout = po.getIdleTimeout();
            if (myIdleTimeout > 0) {
                openFrame.setIdleTimeOut(new Milliseconds(myIdleTimeout));
                idleTimeoutDelay = po.getIdleTimeout() / 2;
                idleTimeoutChecker = new TimerListener() {
                    public void performTimeAction(TimerEvent evt) {
                        dispatch(new POCheckIdleTimeout(null));
                    }
                };
                TimerRegistry.Singleton().addTimerListener(idleTimeoutDelay, idleTimeoutChecker);
            }
            outboundHandler.send(openFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }
        checkBothSidesOpen();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POConnectionFrameReceived po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        if (po.isSasl())
            ((SaslFrameIF) po.getFrame()).accept(connectionVisitor);
        else
            ((FrameIF) po.getFrame()).accept(connectionVisitor);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendHeartBeat po) {
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", visit, po=" + po + " ...");
        outboundHandler.send(HEARTBEAT_FRAME);
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POCheckIdleTimeout po) {
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", visit, po=" + po + " ...");
        long to = lastActivity + myIdleTimeout;
        if (System.currentTimeMillis() > to) {
            pTracer.trace(toString(), ", idleTimeout reached (" + myIdleTimeout + " ms). Closing connection!");
            new Disconnecter(ConnectionError.CONNECTION_FORCED.getValue(), "IdleTimeout reached (" + myIdleTimeout + " ms). Closing connection!").start();
        }
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendClose po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        if (po.getSemaphore() != null)
            closePO = po;
        try {
            CloseFrame closeFrame = new CloseFrame(0);
            if (po.getCondition() != null) {
                com.swiftmq.amqp.v100.generated.transport.definitions.Error error = new com.swiftmq.amqp.v100.generated.transport.definitions.Error();
                error.setCondition(ErrorConditionFactory.create(po.getCondition()));
                if (po.getDescription() != null)
                    error.setDescription(po.getDescription());
                closeFrame.setError(error);
            }
            Semaphore sem = new Semaphore();
            closeFrame.setSemaphore(sem);
            outboundHandler.send(closeFrame);
            sem.waitHere();
        } catch (Exception e) {
            if (po.getSemaphore() != null) {
                po.setException(e.toString());
                po.setSuccess(false);
                po.getSemaphore().notifySingleWaiter();
            }
        }
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POConnectionClose po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        notifyWaitingPOs(new POObject[]{protPO, authPO, openPO, closePO});
        closed = true;
        pipelineQueue.close();
        po.setSuccess(true);
        if (po.getSemaphore() != null)
            po.getSemaphore().notifySingleWaiter();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void close() {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", close ...");
        closeLock.lock();
        try {
            if (closeInProgress) {
                if (pTracer.isEnabled()) pTracer.trace(toString(), ", close in progress, return");
                return;
            }
            closeInProgress = true;
            if (heartBeatSender != null) {
                TimerRegistry.Singleton().removeTimerListener(heartBeatDelay, heartBeatSender);
                heartBeatSender = null;
            }
            if (idleTimeoutChecker != null) {
                TimerRegistry.Singleton().removeTimerListener(idleTimeoutDelay, idleTimeoutChecker);
                idleTimeoutChecker = null;
            }
        } finally {
            closeLock.unlock();
        }
        Semaphore sem = new Semaphore();
        dispatch(new POConnectionClose(sem));
        sem.waitHere();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", close done");
    }

    public String toString() {
        return "ConnectionDispatcher";
    }

    private class DispatchVisitor implements FrameVisitor, SaslFrameVisitor {

        public void visit(OpenFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }

        public void visit(BeginFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }

        public void visit(AttachFrame frame) {
            dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
        }

        public void visit(FlowFrame frame) {
            dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
        }

        public void visit(TransferFrame frame) {
            dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
        }

        public void visit(DispositionFrame frame) {
            dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
        }

        public void visit(DetachFrame frame) {
            dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
        }

        public void visit(EndFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }

        public void visit(CloseFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }

        public void visit(SaslMechanismsFrame frame) {
            dispatch(new POConnectionFrameReceived(frame, true));
        }

        public void visit(SaslInitFrame frame) {
            dispatch(new POConnectionFrameReceived(frame, true));
        }

        public void visit(SaslChallengeFrame frame) {
            dispatch(new POConnectionFrameReceived(frame, true));
        }

        public void visit(SaslResponseFrame frame) {
            dispatch(new POConnectionFrameReceived(frame, true));
        }

        public void visit(SaslOutcomeFrame frame) {
            dispatch(new POConnectionFrameReceived(frame, true));
            awaitProtocolHeader = true;
            protocolHandler.setProtHeaderExpected(true);
            saslActive = false;
        }

        public void visit(HeartbeatFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }
    }

    private class ConnectionVisitor implements FrameVisitor, SaslFrameVisitor {
        public void visit(OpenFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            remoteOpen = frame;
            checkBothSidesOpen();
            if (frame.getMaxFrameSize() != null)
                maxRemoteFrameSize = frame.getMaxFrameSize().getValue() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) frame.getMaxFrameSize().getValue();
            Milliseconds idleTimeout = frame.getIdleTimeOut();
            if (idleTimeout != null) {
                heartBeatDelay = idleTimeout.getValue() / 2;
                if (heartBeatDelay > 0) {
                    heartBeatSender = new TimerListener() {
                        public void performTimeAction(TimerEvent evt) {
                            dispatch(new POSendHeartBeat(null));
                        }
                    };
                    TimerRegistry.Singleton().addTimerListener(heartBeatDelay, heartBeatSender);
                }
            }
        }

        public void visit(BeginFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            try {
                int remoteChannel = frame.getChannel();
                if (frame.getRemoteChannel() != null) {
                    Session session = myConnection.getSessionForLocalChannel(frame.getRemoteChannel().getValue());
                    if (session != null) {
                        myConnection.mapSessionToRemoteChannel(session, remoteChannel);
                        session.setRemoteChannel(remoteChannel);
                        session.dispatch(new POSessionFrameReceived(frame));
                    } else if (pTracer.isEnabled())
                        pTracer.trace(toString(), ", invalid channel (no associated session): " + remoteChannel);
                } else if (pTracer.isEnabled()) pTracer.trace(toString(), ", local channel field not set");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void visit(AttachFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(FlowFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(TransferFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(DispositionFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(DetachFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(EndFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            if (frame.getError() == null)
                dispatchSession(frame.getChannel(), new POSessionFrameReceived(frame));
            else {
                Session session = myConnection.getSessionForRemoteChannel(frame.getChannel());
                if (session != null)
                    session.remoteEnd(frame.getError());
            }
        }

        public void visit(CloseFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            remoteClose = frame;
            checkBothSidesClosed();
        }

        public void visit(SaslMechanismsFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            saslMechanisms = frame;
            checkStartSaslInit();
        }

        public void visit(SaslInitFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslChallengeFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            try {
                byte[] response = saslClient.evaluateChallenge(frame.getChallenge().getValue());
                SaslResponseFrame responseFrame = new SaslResponseFrame(0);
                responseFrame.setResponse(new AMQPBinary(response));
                outboundHandler.send(responseFrame);
            } catch (SaslException e) {
                e.printStackTrace();
                authPO.setSuccess(false);
                authPO.setException(e.toString());
                authPO.getSemaphore().notifySingleWaiter();
            }
        }

        public void visit(SaslResponseFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslOutcomeFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            SaslCode code = frame.getMycode();
            if (code.getValue() == SaslCode.OK.getValue())
                authPO.setSuccess(true);
            else {
                authPO.setSuccess(false);
                authPO.setException("AuthenticationException: SASLOutcome code=" + code.getValue());
            }
            authPO.getSemaphore().notifySingleWaiter();
        }

        public void visit(HeartbeatFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public String toString() {
            return "ConnectionVisitor";
        }
    }

    private class CBHandler implements CallbackHandler {
        String username = null;
        String password = null;

        private CBHandler(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callbacks[i];
                    nc.setName(username);

                } else if (callbacks[i] instanceof PasswordCallback) {
                    // prompt the user for sensitive information
                    PasswordCallback pc = (PasswordCallback) callbacks[i];
                    pc.setPassword(password.toCharArray());
                } else if (callbacks[i] instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callbacks[i];
                    rc.setText(remoteHostname);
                } else {
                    throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback");
                }
            }
        }
    }

    private class Disconnecter extends Thread {
        String condition;
        String description;

        private Disconnecter(String condition, String description) {
            this.condition = condition;
            this.description = description;
        }

        public void run() {
            myConnection.close(condition, description);
        }
    }
}
