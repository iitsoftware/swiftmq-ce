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

package com.swiftmq.impl.amqp.amqp.v01_00_00;

import com.swiftmq.amqp.v100.generated.FrameReader;
import com.swiftmq.amqp.v100.generated.transport.definitions.AmqpError;
import com.swiftmq.amqp.v100.generated.transport.definitions.ConnectionError;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.transport.AMQPFrame;
import com.swiftmq.amqp.v100.transport.HeartbeatFrame;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.Handler;
import com.swiftmq.impl.amqp.OutboundTracer;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.VersionedConnection;
import com.swiftmq.impl.amqp.amqp.v01_00_00.po.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.LengthCaptureDataInput;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AMQPHandler extends FrameVisitorAdapter implements Handler, AMQPConnectionVisitor {
    static final HeartbeatFrame HEARTBEAT_FRAME = new HeartbeatFrame(0);
    static final String VERSION = "1.0.0";
    private static final String CAPABILITY_NO_LOCAL = "APACHE.ORG:NO_LOCAL";
    private static final String CAPABILITY_SELECTOR = "APACHE.ORG:SELECTOR";
    SwiftletContext ctx = null;
    VersionedConnection versionedConnection = null;
    PipelineQueue pipelineQueue = null;
    boolean closed = false;
    boolean closeInProgress = false;
    Lock closeLock = new ReentrantLock();
    String hostname = null;
    DispatchVisitorAdapter dispatchVisitorAdapter = new DispatchVisitorAdapter();
    ConnectionVisitorAdapter connectionVisitorAdapter = new ConnectionVisitorAdapter();
    long lastActivity = System.currentTimeMillis();
    long myIdleTimeout = -1;
    TimerListener heartBeatSender = null;
    TimerListener idleTimeoutChecker = null;
    boolean closeFrameSent = false;
    ArrayList localChannels = new ArrayList();
    ArrayList remoteChannels = new ArrayList();
    Entity usage = null;
    EntityList sessionUsageList = null;
    Property receivedSecProp = null;
    Property sentSecProp = null;
    Property receivedTotalProp = null;
    Property sentTotalProp = null;
    int maxLocalFrameSize = Integer.MAX_VALUE;
    int maxRemoteFrameSize = Integer.MAX_VALUE;
    boolean connectionDisabled = false;
    boolean opened = false;
    boolean apacheSelectors = false;

    public AMQPHandler(SwiftletContext ctx, VersionedConnection versionedConnection) {
        this.ctx = ctx;
        this.versionedConnection = versionedConnection;
        versionedConnection.setOutboundTracer(new OutboundTracer() {
            public String getTraceKey() {
                return "amqp-100";
            }

            public String getTraceString(Object obj) {
                if (obj instanceof AMQPFrame)
                    return "[" + ((AMQPFrame) obj).getChannel() + "] (size=" + ((AMQPFrame) obj).getPredictedSize() + "): " + obj;
                else
                    return obj.toString();
            }
        });
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
        }
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(VersionedConnection.TP_CONNECTIONSVC), "AMQPHandler", this);
        usage = versionedConnection.getUsage();
        if (!ctx.smartTree)
            sessionUsageList = (EntityList) usage.getEntity("sessions");
        receivedSecProp = usage.getProperty("msgs-received");
        sentSecProp = usage.getProperty("msgs-sent");
        receivedTotalProp = usage.getProperty("total-received");
        sentTotalProp = usage.getProperty("total-sent");
        maxLocalFrameSize = ((Long) versionedConnection.getConnectionTemplate().getProperty("max-frame-size").getValue()).intValue();
        dispatch(new POSendOpen());
    }

    protected void mapSessionHandlerToRemoteChannel(SessionHandler sessionHandler, int remoteChannel) {
        Util.ensureSize(remoteChannels, remoteChannel + 1);
        remoteChannels.set(remoteChannel, sessionHandler);
    }

    protected void unmapSessionHandlerFromRemoteChannel(int remoteChannel) {
        Util.ensureSize(remoteChannels, remoteChannel + 1);
        remoteChannels.set(remoteChannel, null);
    }

    protected SessionHandler getSessionHandlerForRemoteChannel(int remoteChannel) {
        if (remoteChannel >= 0 && remoteChannel < remoteChannels.size())
            return (SessionHandler) remoteChannels.get(remoteChannel);
        return null;
    }

    public int getMaxFrameSize() {
        // smallest size of local/remote size but at least 512
        return Math.max(512, Math.min(maxLocalFrameSize, maxRemoteFrameSize));
    }

    public boolean isApacheSelectors() {
        return apacheSelectors;
    }

    public void dispatch(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void dataAvailable(LengthCaptureDataInput in) {
        try {
            lastActivity = System.currentTimeMillis();
            FrameIF frame = FrameReader.createFrame(in);
            if (connectionDisabled) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, connection is disabled");
                return;
            }
            int size = frame.getPredictedSize();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, frame=" + frame);
            if (ctx.protSpace.enabled)
                ctx.protSpace.trace("amqp-100", toString() + "/RCV[" + ((AMQPFrame) frame).getChannel() + "] (size=" + size + "): " + frame);
            if (size > maxLocalFrameSize) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")");
                ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), "Connection closed. Error: Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")");
                connectionDisabled = true;
                dispatch(new POSendClose(ConnectionError.FRAMING_ERROR, new AMQPString("Frame size (" + size + ") > max frame size (" + maxLocalFrameSize + ")")));
            } else
                frame.accept(dispatchVisitorAdapter);
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", dataAvailable, exception=" + e);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
        }
    }

    public void collect(long lastCollect) {
        dispatch(new POConnectionCollect(lastCollect));
    }

    public String getVersion() {
        return VERSION;
    }

    public void visit(POSendOpen po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        Entity connectionTemplate = versionedConnection.getConnectionTemplate();
        myIdleTimeout = ((Long) connectionTemplate.getProperty("idle-timeout").getValue()).longValue();
        OpenFrame frame = new OpenFrame(0);
        frame.setContainerId(new AMQPString(SwiftletManager.getInstance().getRouterName()));
        frame.setChannelMax(new AMQPUnsignedShort(((Integer) connectionTemplate.getProperty("max-channel-number").getValue()).intValue()));
        frame.setMaxFrameSize(new AMQPUnsignedInt(((Long) connectionTemplate.getProperty("max-frame-size").getValue()).longValue()));
        try {
            AMQPArray offeredCapas = new AMQPArray(AMQPTypeDecoder.SYM8, new AMQPSymbol[]{new AMQPSymbol(CAPABILITY_NO_LOCAL), new AMQPSymbol(CAPABILITY_SELECTOR)});
            frame.setOfferedCapabilities(offeredCapas);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (myIdleTimeout > 0) {
            frame.setIdleTimeOut(new Milliseconds(myIdleTimeout));
            idleTimeoutChecker = new TimerListener() {
                public void performTimeAction() {
                    dispatch(new POCheckIdleTimeout(null));
                }
            };
            ctx.timerSwiftlet.addTimerListener(myIdleTimeout / 2, idleTimeoutChecker);
        }
        versionedConnection.send(AMQPHandlerFactory.AMQP_INIT);
        versionedConnection.send(frame);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POConnectionFrameReceived po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        po.getFrame().accept(connectionVisitorAdapter);
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendHeartBeat po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        versionedConnection.send(HEARTBEAT_FRAME);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POCheckIdleTimeout po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        long to = lastActivity + myIdleTimeout;
        if (System.currentTimeMillis() > to) {
            ctx.logSwiftlet.logWarning(ctx.amqpSwiftlet.getName(), toString() + ", idleTimeout reached (" + myIdleTimeout + " ms). Closing connection!");
            ctx.networkSwiftlet.getConnectionManager().removeConnection(versionedConnection.getConnection());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POSendClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        CloseFrame frame = new CloseFrame(0);
        if (po.getErrorCondition() != null) {
            com.swiftmq.amqp.v100.generated.transport.definitions.Error error = new Error();
            error.setCondition(po.getErrorCondition());
            if (po.getDescription() != null)
                error.setDescription(po.getDescription());
            frame.setError(error);
            connectionDisabled = true;
        }
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

    public void visit(POConnectionCollect po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        long received = 0;
        long sent = 0;
        int totalReceived = 0;
        int totalSent = 0;
        for (int i = 0; i < localChannels.size(); i++) {
            SessionHandler sessionHandler = (SessionHandler) localChannels.get(i);
            if (sessionHandler != null) {
                received += sessionHandler.getMsgsReceived();
                sent += sessionHandler.getMsgsSent();
                totalReceived += sessionHandler.getTotalMsgsReceived();
                totalSent += sessionHandler.getTotalMsgsSent();
                sessionHandler.collect(po.getLastCollect());
            }
        }
        double deltasec = Math.max(1.0, (double) (System.currentTimeMillis() - po.getLastCollect()) / 1000.0);
        double rsec = ((double) received / (double) deltasec) + 0.5;
        double ssec = ((double) sent / (double) deltasec) + 0.5;
        try {
            if (((Integer) receivedSecProp.getValue()).intValue() != rsec)
                receivedSecProp.setValue(new Integer((int) rsec));
            if (((Integer) sentSecProp.getValue()).intValue() != ssec)
                sentSecProp.setValue(new Integer((int) ssec));
            if (((Integer) receivedTotalProp.getValue()).intValue() != totalReceived)
                receivedTotalProp.setValue(new Integer(totalReceived));
            if (((Integer) sentTotalProp.getValue()).intValue() != totalSent)
                sentTotalProp.setValue(new Integer(totalSent));
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " done");
    }

    public void visit(POClose po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit, po=" + po + " ...");
        ctx.removeId(versionedConnection.getActiveLogin().getClientId());
        if (heartBeatSender != null) {
            ctx.timerSwiftlet.removeTimerListener(heartBeatSender);
            heartBeatSender = null;
        }
        if (idleTimeoutChecker != null) {
            ctx.timerSwiftlet.removeTimerListener(idleTimeoutChecker);
            idleTimeoutChecker = null;
        }
        for (int i = 0; i < localChannels.size(); i++) {
            SessionHandler sessionHandler = (SessionHandler) localChannels.get(i);
            if (sessionHandler != null) {
                sessionHandler.close();
                if (sessionUsageList != null)
                    sessionUsageList.removeDynamicEntity(sessionHandler);
            }
        }
        localChannels.clear();
        remoteChannels.clear();
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

    public String toString() {
        return versionedConnection.toString() + "/AMQPHandler/" + VERSION;
    }

    private class DispatchVisitorAdapter extends FrameVisitorAdapter {
        private void dispatchSession(int remoteChannel, POObject po) {
            if (remoteChannel >= 0 && remoteChannel < remoteChannels.size()) {
                SessionHandler sessionHandler = (SessionHandler) remoteChannels.get(remoteChannel);
                if (sessionHandler != null)
                    sessionHandler.dispatch(po);
            }
        }

        public void visit(OpenFrame frame) {
            Semaphore sem = new Semaphore();
            dispatch(new POConnectionFrameReceived(sem, frame));
            sem.waitHere();
        }

        public void visit(BeginFrame frame) {
            Semaphore sem = new Semaphore();
            dispatch(new POConnectionFrameReceived(sem, frame));
            sem.waitHere();
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
            Semaphore sem = new Semaphore();
            dispatch(new POConnectionFrameReceived(sem, frame));
            sem.waitHere();
        }

        public void visit(CloseFrame frame) {
            Semaphore sem = new Semaphore();
            dispatch(new POConnectionFrameReceived(sem, frame));
            sem.waitHere();
        }

        public void visit(HeartbeatFrame frame) {
            dispatch(new POConnectionFrameReceived(frame));
        }

        public String toString() {
            return AMQPHandler.this.toString() + "/ConnectionDispatcher";
        }
    }

    private class ConnectionVisitorAdapter extends FrameVisitorAdapter {
        private boolean checkCapability(String capa, AMQPArray array) throws IOException {
            if (array == null)
                return false;
            AMQPType[] tarray = array.getValue();
            if (tarray == null)
                return false;
            for (int i = 0; i < tarray.length; i++) {
                String val = ((AMQPSymbol) tarray[i]).getValue();
                if (val.toUpperCase().equals(capa))
                    return true;
            }
            return false;
        }

        public void visit(OpenFrame frame) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit OpenFrame ...");
            try {
                String cid = frame.getContainerId() != null ? frame.getContainerId().getValue() : null;
                ctx.addConnectId(cid);
                versionedConnection.getActiveLogin().setClientId(cid);
                versionedConnection.getUsage().getProperty("container-id").setValue(cid);

                if (frame.getMaxFrameSize() != null)
                    maxRemoteFrameSize = frame.getMaxFrameSize().getValue() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) frame.getMaxFrameSize().getValue();

                AMQPArray offeredCapas = frame.getOfferedCapabilities();
                AMQPArray desiredCapas = frame.getDesiredCapabilities();
                apacheSelectors = checkCapability(CAPABILITY_SELECTOR, offeredCapas) || checkCapability(CAPABILITY_SELECTOR, desiredCapas);
                Milliseconds idleTimeout = frame.getIdleTimeOut();
                if (idleTimeout != null) {
                    long value = idleTimeout.getValue() / 2;
                    if (value > 0) {
                        heartBeatSender = new TimerListener() {
                            public void performTimeAction() {
                                dispatch(new POSendHeartBeat(null));
                            }
                        };
                        ctx.timerSwiftlet.addTimerListener(value / 2, heartBeatSender);
                    }
                }
                opened = true;
            } catch (Exception e) {
                dispatch(new POSendClose(AmqpError.ILLEGAL_STATE, new AMQPString(e.getMessage())));
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit OpenFrame done");
        }

        public void visit(BeginFrame frame) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit BeginFrame ...");
            SessionHandler sessionHandler = new SessionHandler(ctx, versionedConnection, AMQPHandler.this, frame);
            sessionHandler.setChannel(ArrayListTool.setFirstFreeOrExpand(localChannels, sessionHandler));
            if (sessionUsageList != null) {
                Entity sessionUsage = sessionUsageList.createEntity();
                sessionUsage.setName(String.valueOf(sessionHandler.getChannel()));
                sessionUsage.createCommands();
                try {
                    sessionUsage.setDynamicObject(sessionHandler);
                    sessionUsageList.addEntity(sessionUsage);
                    sessionUsage.getProperty("remote-channel").setValue(new Integer(frame.getChannel()));
                } catch (Exception e) {
                }
                sessionHandler.setUsage(sessionUsage);
            }
            sessionHandler.startSession();
            mapSessionHandlerToRemoteChannel(sessionHandler, frame.getChannel());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit BeginFrame done");
        }

        public void visit(EndFrame frame) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit EndFrame ...");

            SessionHandler sessionHandler = getSessionHandlerForRemoteChannel(frame.getChannel());
            if (sessionHandler != null) {
                localChannels.set(sessionHandler.getChannel(), null);
                unmapSessionHandlerFromRemoteChannel(frame.getChannel());
                sessionHandler.stopSession();
                sessionHandler.close();
                if (sessionUsageList != null)
                    sessionUsageList.removeDynamicEntity(sessionHandler);
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit EndFrame: Illegal state: no session handler found for remote channel: " + frame.getChannel() + ". Disconnecting.");
                ctx.logSwiftlet.logError(ctx.amqpSwiftlet.getName(), toString() + ", visit EndFrame: Illegal state: no session handler found for remote channel: " + frame.getChannel() + ". Disconnecting.");
                dispatch(new POSendClose(AmqpError.ILLEGAL_STATE, new AMQPString("Illegal state: no session handler found for remote channel: " + frame.getChannel() + ". Disconnecting.")));
            }

            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit EndFrame done");
        }

        public void visit(CloseFrame frame) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit CloseFrame ...");

            if (!closeFrameSent)
                dispatch(new POSendClose(null, null));

            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit CloseFrame done");
        }

        public void visit(HeartbeatFrame frame) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit HeartbeatFrame ...");
            lastActivity = System.currentTimeMillis();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", visit HeartbeatFrame done");
        }

        public String toString() {
            return AMQPHandler.this.toString() + "/ConnectionVisitorAdapter";
        }
    }
}
