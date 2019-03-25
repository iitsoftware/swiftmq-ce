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
import com.swiftmq.amqp.integration.Tracer;
import com.swiftmq.amqp.v100.client.po.*;
import com.swiftmq.amqp.v100.generated.filter.filter_types.NoLocalFilter;
import com.swiftmq.amqp.v100.generated.filter.filter_types.SelectorFilter;
import com.swiftmq.amqp.v100.generated.messaging.addressing.*;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Accepted;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Rejected;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.security.sasl.*;
import com.swiftmq.amqp.v100.generated.transactions.coordination.Coordinator;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionalState;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnCapability;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.generated.transport.performatives.*;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.transport.HeartbeatFrame;
import com.swiftmq.amqp.v100.transport.Packager;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.IdGenerator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SessionDispatcher
        implements SessionVisitor {
    AMQPContext ctx = null;
    Tracer fTracer = null;
    Tracer pTracer = null;
    Session mySession = null;
    boolean transacted = false;
    int myChannel = 0;
    FrameVisitor visitor = new SessionDispatcherFrameVisitor();
    OutboundHandler outboundHandler = null;
    PipelineQueue pipelineQueue = null;
    POBegin beginPO = null;
    BeginFrame remoteBegin = null;
    POSendEnd endPO = null;
    EndFrame remoteEnd = null;
    boolean closed = false;
    boolean closeInProgress = false;
    Lock closeLock = new ReentrantLock();
    ArrayList handles = new ArrayList();
    Map remoteHandles = new HashMap();
    Map waitingPO = new HashMap();
    Map<Long, DeliveryMapping> unsettledOutgoingDeliveries = new HashMap<Long, DeliveryMapping>();
    Map<Long, DeliveryMapping> unsettledIncomingDeliveries = new HashMap<Long, DeliveryMapping>();
    List outboundDeliveries = new ArrayList();
    long nextLinkId = 0;
    String uniqueSessionId = IdGenerator.getInstance().nextId('/');
    boolean windowChanged = false;

    long initialOutgoingId = 1;
    long nextIncomingId = 0;
    long incomingWindow = 0;
    long nextOutgoingId = initialOutgoingId;
    long outgoingWindow = 0;
    long remoteIncomingWindow = 0;
    long remoteOutgoingWindow = 0;
    long deliveryId = initialOutgoingId;

    public SessionDispatcher(AMQPContext ctx, Session mySession, OutboundHandler outboundHandler) {
        this.ctx = ctx;
        this.mySession = mySession;
        this.outboundHandler = outboundHandler;
        fTracer = ctx.getFrameTracer();
        pTracer = ctx.getProcessingTracer();
        pipelineQueue = new PipelineQueue(ctx.getSessionPool(), "SessionDispatcher", this);
        incomingWindow = mySession.getIncomingWindowSize();
        outgoingWindow = mySession.getOutgoingWindowSize();
    }

    private long nextDeliveryId() {
        if (deliveryId == Long.MAX_VALUE)
            deliveryId = initialOutgoingId;
        else
            deliveryId++;
        return deliveryId;
    }

    private void checkBothSidesBegin() {
        if (beginPO != null && remoteBegin != null) {
            beginPO.setSuccess(true);
            beginPO.getSemaphore().notifySingleWaiter();
            beginPO = null;
        }
    }

    private void checkBothSidesEnd() {
        if (endPO != null && remoteEnd != null) {
            endPO.setSuccess(true);
            endPO.getSemaphore().notifySingleWaiter();
            endPO = null;
        }
    }

    private void sendFlow() {
        FlowFrame flowFrame = new FlowFrame(mySession.getChannel());
        flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
        flowFrame.setNextIncomingId(new TransferNumber(nextIncomingId));
        flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
        flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
        flowFrame.setDrain(AMQPBoolean.FALSE);
        flowFrame.setEcho(AMQPBoolean.FALSE);
        outboundHandler.send(flowFrame);
    }

    private void doSend(POSendMessage po) {
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", doSend, po=" + po + ", dataLength=" + po.getPackager().getSize());
        try {
            Producer producer = po.getProducer();
            producer.verifyState();
            Packager packager = po.getPackager();
            if (remoteIncomingWindow > 0 && outgoingWindow > 0) {
                do {
                    boolean wasFirstPacket = false;
                    boolean isAtMostOnce = producer.getQoS() == QoS.AT_MOST_ONCE;
                    packager.setMaxFrameSize(mySession.myConnection.connectionDispatcher.getMaxFrameSize());
                    TransferFrame frame = new TransferFrame(mySession.getChannel());
                    frame.setHandle(new Handle(producer.getHandle()));
                    frame.setSettled(new AMQPBoolean(isAtMostOnce));
                    if (packager.getCurrentPacketNumber() == 0) {
                        long dId = nextDeliveryId();
                        wasFirstPacket = true;
                        producer.incDeliveryCountSnd();
                        DeliveryTag deliveryTag = po.getDeliveryTag() != null ? po.getDeliveryTag() : producer.createDeliveryTag();
                        if (!isAtMostOnce) {
                            if (po.getTxnId() == null && !po.isRecovery())
                                producer.getDeliveryMemory().addUnsettledDelivery(new UnsettledDelivery(deliveryTag, null, po.getMessage()));
                            unsettledOutgoingDeliveries.put(dId, new DeliveryMapping(deliveryTag, producer));
                        }
                        frame.setDeliveryTag(deliveryTag);
                        frame.setDeliveryId(new DeliveryNumber(dId));
                        frame.setMessageFormat(new MessageFormat(0));
                        TxnIdIF txnId = po.getTxnId();
                        if (txnId != null) {
                            TransactionalState txState = new TransactionalState();
                            txState.setTxnId(txnId);
                            frame.setState(txState);
                        }
                    }
                    packager.setMessageFormat(0);
                    packager.getNextPacket(frame);
                    // We may increase the outgoing window and send a flow before
                    if (wasFirstPacket && outgoingWindow - packager.getPredictedNumberPackets() < 0) {
                        outgoingWindow += packager.getPredictedNumberPackets();
                        sendFlow();
                        windowChanged = true;
                    }
                    if (pTracer.isEnabled())
                        pTracer.trace(toString(), ", doSend, remoteIncomingWindows=" + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", sending message, wasFirstPacket=" + wasFirstPacket + ", maxSize=" + packager.getMaxPayloadLength() + ", packetSize=" + frame.getPayload().length + ", predictedNumberPackets=" + packager.getPredictedNumberPackets() + ", currentPacket=" + packager.getCurrentPacketNumber() + ", hasMore=" + packager.hasMore());

                    outboundHandler.send(frame);
                    nextOutgoingId++;
                    remoteIncomingWindow--;
                    if (!isAtMostOnce)
                        outgoingWindow--;
                    if (!packager.hasMore()) {
                        // Release of the send method:
                        // a) Messages for transaction controller or transacted messages: after settlement
                        // b) everything else: after the last packet has been sent
                        if (producer.isTransactionController() || po.getTxnId() != null)
                            producer.setWaitingPO(po);
                        else {
                            po.setSuccess(true);
                            if (po.getSemaphore() != null)
                                po.getSemaphore().notifySingleWaiter();
                        }
                        // If that was the last packet and outgoing window was increased for this message, we need to reset it and send another flow
                        if (windowChanged) {
                            outgoingWindow = mySession.getOutgoingWindowSize();
                            sendFlow();
                        }

                        break;
                    }
                } while (remoteIncomingWindow > 0 && outgoingWindow > 0);
                if (packager.hasMore()) {
                    if (pTracer.isEnabled())
                        pTracer.trace(toString(), ", doSend, remoteIncomingWindows=" + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", has more but no window, storing message");
                    outboundDeliveries.add(po);
                }
            } else {
                if (pTracer.isEnabled())
                    pTracer.trace(toString(), ", doSend, po=" + po + ", remoteIncomingWindows=" + remoteIncomingWindow + ", outgoingWindow=" + outgoingWindow + ", no window, storing message");
                outboundDeliveries.add(po);
            }
        } catch (Exception e) {
            po.setSuccess(false);
            po.setException(e.getMessage());
            if (po.getSemaphore() != null)
                po.getSemaphore().notifySingleWaiter();
        }
    }

    private void settleOutbound(long from, long to, boolean settled, DeliveryStateIF deliveryState) {
        if (from <= to) {
            long current = from;
            while (current <= to) {
                DeliveryMapping deliveryMapping = unsettledOutgoingDeliveries.remove(current);
                if (deliveryMapping != null) {
                    deliveryMapping.link.getDeliveryMemory().deliverySettled(deliveryMapping.deliveryTag);
                    if (deliveryMapping.link.getWaitingPO() != null) {
                        POSendMessage po = (POSendMessage) deliveryMapping.link.getWaitingPO();
                        po.setSuccess(true);
                        po.setDeliveryState(deliveryState);
                        po.getSemaphore().notifySingleWaiter();
                        deliveryMapping.link.setWaitingPO(null);
                    }
                    // If there is a close waiting on that link, dispatch it when there are no more unsettled deliveries
                    if (deliveryMapping.link.getDeliveryMemory().getNumberUnsettled() == 0 && deliveryMapping.link.getWaitingClosePO() != null) {
                        dispatch(deliveryMapping.link.getWaitingClosePO());
                        deliveryMapping.link.setWaitingClosePO(null);
                    }
                }
                current++;
                outgoingWindow++;
            }
            if (deliveryState != null) {
                if (!settled && deliveryState instanceof Accepted) {
                    DispositionFrame dispoFrame = new DispositionFrame(mySession.getChannel());
                    dispoFrame.setRole(Role.SENDER);
                    dispoFrame.setFirst(new DeliveryNumber(from));
                    dispoFrame.setLast(new DeliveryNumber(to));
                    dispoFrame.setSettled(AMQPBoolean.TRUE);
                    dispoFrame.setState(new Accepted());
                    outboundHandler.send(dispoFrame);
                }
            }
        } else {
            // TODO: error
        }
    }

    private void settleInbound(long from, long to, boolean settled) {
        if (from <= to) {
            long current = from;
            while (current <= to) {
                DeliveryMapping deliveryMapping = unsettledIncomingDeliveries.remove(current);
                if (deliveryMapping != null) {
                    deliveryMapping.link.getDeliveryMemory().deliverySettled(deliveryMapping.deliveryTag);
                    if (deliveryMapping.link.getDeliveryMemory().getNumberUnsettled() == 0 && deliveryMapping.link.getWaitingClosePO() != null) {
                        dispatch(deliveryMapping.link.getWaitingClosePO());
                        deliveryMapping.link.setWaitingClosePO(null);
                    }
                }
                current++;
            }
        } else {
            // TODO: error
        }
    }

    private void notifyWaitingPOs(POObject[] po) {
        String msg = "Session was asynchronously closed";
        for (int i = 0; i < handles.size(); i++) {
            Link link = (Link) handles.get(i);
            if (link != null) {
                if (link.getWaitingPO() != null && link.getWaitingPO().getSemaphore() != null) {
                    link.getWaitingPO().setSuccess(false);
                    link.getWaitingPO().setException(msg);
                    link.getWaitingPO().getSemaphore().notifySingleWaiter();
                }
                if (link.getWaitingClosePO() != null && link.getWaitingClosePO().getSemaphore() != null) {
                    link.getWaitingClosePO().setSuccess(false);
                    link.getWaitingClosePO().setException(msg);
                    link.getWaitingClosePO().getSemaphore().notifySingleWaiter();
                }
            }
        }
        for (Iterator iter = waitingPO.entrySet().iterator(); iter.hasNext(); ) {
            POObject wpo = (POObject) ((Map.Entry) iter.next()).getValue();
            if (wpo != null && wpo.getSemaphore() != null) {
                wpo.setSuccess(false);
                wpo.setException(msg);
                wpo.getSemaphore().notifySingleWaiter();
            }
        }
        for (int i = 0; i < po.length; i++) {
            if (po[i] != null && po[i].getSemaphore() != null) {
                po[i].setSuccess(false);
                po[i].setException(msg);
                po[i].getSemaphore().notifySingleWaiter();
            }
        }
    }

    private void removeDeliveries(Link link, Map map) {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry entry = (Map.Entry) iter.next();
            DeliveryMapping deliveryMapping = (DeliveryMapping) entry.getValue();
            if (deliveryMapping.link == link)
                iter.remove();
        }
    }

    private AMQPMap getUnsettledMap(DeliveryMemory deliveryMemory) throws IOException {
        Collection<UnsettledDelivery> unsettledDeliveries = deliveryMemory.getUnsettled();
        if (unsettledDeliveries == null || unsettledDeliveries.size() == 0)
            return null;
        Map<AMQPType, AMQPType> dmap = new HashMap<AMQPType, AMQPType>(unsettledDeliveries.size());
        for (Iterator<UnsettledDelivery> iter = unsettledDeliveries.iterator(); iter.hasNext(); ) {
            UnsettledDelivery unsettledDelivery = iter.next();
            dmap.put(unsettledDelivery.getDeliveryTag(), unsettledDelivery.getDeliveryStateIF() != null ? (AMQPType) unsettledDelivery.getDeliveryStateIF() : new AMQPNull());
        }
        return new AMQPMap(dmap);
    }

    public void setMyChannel(int myChannel) {
        this.myChannel = myChannel;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public void dispatch(POObject po) {
        pipelineQueue.enqueue(po);
    }

    public void visit(POBegin po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        beginPO = po;
        try {
            BeginFrame beginFrame = new BeginFrame(mySession.getChannel());
            beginFrame.setHandleMax(new Handle(Integer.MAX_VALUE));
            beginFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
            beginFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
            beginFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
            outboundHandler.send(beginFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }
        checkBothSidesBegin();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POAttachProducer po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        String name = null;
        DeliveryMemory deliveryMemory = po.getDeliveryMemory();
        if (deliveryMemory.getLinkName() != null)
            name = deliveryMemory.getLinkName();
        else {
            name = uniqueSessionId + "/" + po.getTarget() + "/" + (nextLinkId++);
            deliveryMemory.setLinkName(name);
        }
        Producer producer = new Producer(mySession, po.getTarget(), name, po.getQoS(), deliveryMemory);
        int handle = ArrayListTool.setFirstFreeOrExpand(handles, producer);
        producer.setHandle(handle);
        po.setLink(producer);
        waitingPO.put(name, po);
        try {
            AttachFrame attachFrame = new AttachFrame(mySession.getChannel());
            attachFrame.setName(new AMQPString(name));
            attachFrame.setHandle(new Handle(handle));
            attachFrame.setRole(Role.SENDER);
            switch (producer.getQoS()) {
                case QoS.AT_LEAST_ONCE:
                    attachFrame.setRcvSettleMode(ReceiverSettleMode.FIRST);
                    break;
                case QoS.AT_MOST_ONCE:
                    attachFrame.setSndSettleMode(SenderSettleMode.SETTLED);
                    break;
                case QoS.EXACTLY_ONCE:
                    attachFrame.setRcvSettleMode(ReceiverSettleMode.SECOND);
                    break;
            }
            Source source = new Source();
            source.setAddress(new AddressString(name));
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setTimeout(new Seconds(0));
            attachFrame.setSource(source);
            String t = po.getTarget();
            if (t.equals(Coordinator.DESCRIPTOR_NAME)) {
                Coordinator coordinator = new Coordinator();
                coordinator.setCapabilities(new AMQPArray(AMQPTypeDecoder.SYM8, new AMQPType[]{TxnCapability.LOCAL_TRANSACTIONS}));
                attachFrame.setTarget(coordinator);
            } else {
                Target target = new Target();
                target.setAddress(new AddressString(t));
                target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
                target.setTimeout(new Seconds(0));
                attachFrame.setTarget(target);
            }
            attachFrame.setInitialDeliveryCount(new SequenceNo(producer.getDeliveryCountSnd()));
            attachFrame.setUnsettled(getUnsettledMap(producer.getDeliveryMemory()));
            outboundHandler.send(attachFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POAttachConsumer po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        String name = null;
        DeliveryMemory deliveryMemory = po.getDeliveryMemory();
        if (deliveryMemory.getLinkName() != null)
            name = deliveryMemory.getLinkName();
        else {
            name = uniqueSessionId + "/" + po.getSource() + "/" + (nextLinkId++);
            deliveryMemory.setLinkName(name);
        }
        Consumer consumer = null;
        if (po.getLinkCredit() == -1)
            consumer = new Consumer(mySession, po.getSource(), name, po.getQoS(), deliveryMemory);
        else
            consumer = new Consumer(mySession, po.getSource(), name, po.getLinkCredit(), po.getQoS(), deliveryMemory);
        int handle = ArrayListTool.setFirstFreeOrExpand(handles, consumer);
        consumer.setHandle(handle);
        po.setLink(consumer);
        waitingPO.put(name, po);
        try {
            AttachFrame attachFrame = new AttachFrame(mySession.getChannel());
            attachFrame.setName(new AMQPString(name));
            attachFrame.setHandle(new Handle(handle));
            attachFrame.setRole(Role.RECEIVER);
            if (consumer.getQoS() == QoS.AT_MOST_ONCE)
                attachFrame.setSndSettleMode(SenderSettleMode.SETTLED);
            Source source = new Source();
            String s = po.getSource();
            if (s != null)
                source.setAddress(new AddressString(s));
            else
                source.setDynamic(AMQPBoolean.TRUE);
            source.setDurable(TerminusDurability.NONE);
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            source.setTimeout(new Seconds(0));
            Map m = null;
            if (po.isNoLocal()) {
                m = new HashMap();
                m.put(new AMQPSymbol("no-local-filter"), new NoLocalFilter());
            }
            if (po.getSelector() != null) {
                if (m == null)
                    m = new HashMap();
                m.put(new AMQPSymbol("jms-selector-filter"), new SelectorFilter(po.getSelector()));
            }
            if (m != null)
                source.setFilter(new FilterSet(m));
            attachFrame.setSource(source);
            Target target = new Target();
            target.setAddress(new AddressString(name));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setTimeout(new Seconds(0));
            attachFrame.setTarget(target);
            attachFrame.setUnsettled(getUnsettledMap(consumer.getDeliveryMemory()));
            outboundHandler.send(attachFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POAttachDurableConsumer po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        DeliveryMemory deliveryMemory = po.getDeliveryMemory();
        if (deliveryMemory.getLinkName() != null)
            deliveryMemory.setLinkName(po.getLinkName());
        DurableConsumer consumer = new DurableConsumer(mySession, po.getSource(), po.getLinkName(), po.getLinkCredit(), po.getQoS(), deliveryMemory);
        int handle = ArrayListTool.setFirstFreeOrExpand(handles, consumer);
        consumer.setHandle(handle);
        po.setLink(consumer);
        waitingPO.put(po.getLinkName(), po);
        try {
            AttachFrame attachFrame = new AttachFrame(mySession.getChannel());
            attachFrame.setName(new AMQPString(po.getLinkName()));
            attachFrame.setHandle(new Handle(handle));
            attachFrame.setRole(Role.RECEIVER);
            if (consumer.getQoS() == QoS.AT_MOST_ONCE)
                attachFrame.setSndSettleMode(SenderSettleMode.SETTLED);
            Source source = new Source();
            String s = po.getSource();
            if (s != null)
                source.setAddress(new AddressString(s));
            else
                source.setDynamic(AMQPBoolean.TRUE);
            source.setDurable(TerminusDurability.CONFIGURATION);     // This identifies a durable
            source.setExpiryPolicy(po.getExpiryPolicy());
            source.setTimeout(new Seconds(0));
            Map m = null;
            if (po.isNoLocal()) {
                m = new HashMap();
                m.put(new AMQPSymbol("no-local-filter"), new NoLocalFilter());
            }
            if (po.getSelector() != null) {
                if (m == null)
                    m = new HashMap();
                m.put(new AMQPSymbol("jms-selector-filter"), new SelectorFilter(po.getSelector()));
            }
            if (m != null)
                source.setFilter(new FilterSet(m));
            attachFrame.setSource(source);
            Target target = new Target();
            target.setAddress(new AddressString(uniqueSessionId + "/" + po.getSource() + "/" + (nextLinkId++)));
            target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
            target.setTimeout(new Seconds(0));
            attachFrame.setTarget(target);
            attachFrame.setUnsettled(getUnsettledMap(consumer.getDeliveryMemory()));
            outboundHandler.send(attachFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendMessage po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        Producer producer = po.getProducer();
        long linkCredit = producer.getLinkCredit();
        if (pTracer.isEnabled())
            pTracer.trace(toString(), ", visit, po=" + po + ", linkCredit=" + linkCredit);
        if (linkCredit <= 0)
            producer.setWaitingForFlowReleasePO(po);  // Will be released by the next flow frame
        else
            doSend(po);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendResumedTransfer po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        TransferFrame frame = new TransferFrame(mySession.getChannel());
        frame.setHandle(new Handle(po.getProducer().getHandle()));
        frame.setSettled(new AMQPBoolean(true));
        frame.setResume(new AMQPBoolean(true));
        frame.setDeliveryId(new DeliveryNumber(nextOutgoingId++));
        frame.setDeliveryTag(po.getDeliveryTag());
        frame.setState(new Accepted());
        outboundHandler.send(frame);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendDisposition po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        boolean settled = po.getConsumer().getQoS() == QoS.AT_LEAST_ONCE || po.getConsumer().getQoS() == QoS.AT_MOST_ONCE;
        DispositionFrame dispoFrame = new DispositionFrame(mySession.getChannel());
        dispoFrame.setRole(Role.RECEIVER);
        dispoFrame.setBatchable(AMQPBoolean.TRUE);
        dispoFrame.setFirst(new DeliveryNumber(po.getDeliveryId()));
        dispoFrame.setSettled(new AMQPBoolean(settled));
        dispoFrame.setState(po.getDeliveryState());
        if (po.getConsumer().getQoS() == QoS.EXACTLY_ONCE) {
            if (!(po.getDeliveryState() instanceof TransactionalState))
                po.getConsumer().getDeliveryMemory().addUnsettledDelivery(new UnsettledDelivery(po.getDeliveryTag(), po.getDeliveryState(), null));
            unsettledIncomingDeliveries.put(po.getDeliveryId(), new DeliveryMapping(po.getDeliveryTag(), po.getConsumer()));
        }
        outboundHandler.send(dispoFrame);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POFillCache po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        Consumer c = po.getConsumer();
        FlowFrame flowFrame = new FlowFrame(mySession.getChannel());
        flowFrame.setHandle(new Handle(c.getHandle()));
        flowFrame.setAvailable(new AMQPUnsignedInt(0));
        flowFrame.setDrain(AMQPBoolean.FALSE);
        flowFrame.setNextIncomingId(new TransferNumber(nextIncomingId));
        flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
        flowFrame.setLinkCredit(new AMQPUnsignedInt(po.getLinkCredit()));
        flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
        flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
        if (po.getLastDeliveryId() != -1)
            flowFrame.setDeliveryCount(new SequenceNo(po.getLastDeliveryId()));
        TxnIdIF txnIdIF = po.getTxnIdIF();
        if (txnIdIF != null) {
            Map map = new HashMap();
            map.put(new AMQPSymbol("txn-id"), txnIdIF);
            try {
                flowFrame.setProperties(new Fields(map));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        outboundHandler.send(flowFrame);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSessionFrameReceived po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        po.getFrame().accept(visitor);
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSendEnd po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        endPO = po;
        try {
            EndFrame endFrame = new EndFrame(mySession.getChannel());
            if (po.getCondition() != null) {
                com.swiftmq.amqp.v100.generated.transport.definitions.Error error = new com.swiftmq.amqp.v100.generated.transport.definitions.Error();
                error.setCondition(ErrorConditionFactory.create(new AMQPSymbol(po.getCondition())));
                endFrame.setError(error);
            }
            outboundHandler.send(endFrame);
            checkBothSidesEnd();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POCloseLink po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        Link link = po.getLink();
        link.setWaitingClosePO(po);
        if (link.getDeliveryMemory().getNumberUnsettled() == 0 && link.getWaitingPO() == null) {
            DetachFrame detachFrame = new DetachFrame(mySession.getChannel());
            detachFrame.setHandle(new Handle(link.getHandle()));
            detachFrame.setClosed(new AMQPBoolean(true));
            outboundHandler.send(detachFrame);
        }
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " done");
    }

    public void visit(POSessionClose po) {
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit, po=" + po + " ...");
        notifyWaitingPOs(new POObject[]{beginPO, endPO});
        handles.clear();
        remoteHandles.clear();
        unsettledOutgoingDeliveries.clear();
        unsettledIncomingDeliveries.clear();
        for (int i = 0; i < outboundDeliveries.size(); i++) {
            POSendMessage sm = (POSendMessage) outboundDeliveries.get(i);
            if (sm.getSemaphore() != null) {
                sm.setSuccess(false);
                sm.setException("Session was asynchronously closed");
                sm.getSemaphore().notifySingleWaiter();
            }
        }
        outboundDeliveries.clear();
        waitingPO.clear();
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
        if (closeInProgress) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", close in progress, return");
            return;
        }
        closeInProgress = true;
        closeLock.unlock();
        Semaphore sem = new Semaphore();
        dispatch(new POSessionClose(sem));
        sem.waitHere();
        if (pTracer.isEnabled()) pTracer.trace(toString(), ", close done");
    }

    public String toString() {
        return "SessionDispatcher, channel=" + myChannel;
    }

    private class SessionDispatcherFrameVisitor implements FrameVisitor {
        public void visit(OpenFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(BeginFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            if (frame.getNextOutgoingId() != null)
                nextIncomingId = frame.getNextOutgoingId().getValue();
            if (frame.getIncomingWindow() != null)
                remoteIncomingWindow = frame.getIncomingWindow().getValue();
            if (frame.getOutgoingWindow() != null)
                remoteOutgoingWindow = frame.getOutgoingWindow().getValue();
            remoteBegin = frame;
            checkBothSidesBegin();
        }

        public void visit(AttachFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            String name = frame.getName().getValue();
            POAttach po = (POAttach) waitingPO.remove(name);
            if (po != null) {
                Link link = po.getLink();
                SequenceNo idc = frame.getInitialDeliveryCount();
                if (idc != null)
                    link.setDeliveryCount(idc.getValue());
                else
                    link.setDeliveryCount(0);
                link.setRemoteHandle(frame.getHandle().getValue());
                Source source = (Source) frame.getSource();
                if (source != null)
                    link.setRemoteAddress(source.getAddress());
                try {
                    link.setOfferedCapabilities(toSet(frame.getOfferedCapabilities()));
                    link.setDesiredCapabilities(toSet(frame.getDesiredCapabilities()));
                    if (frame.getRole().getValue() == Role.SENDER.getValue()) {
                        if (frame.getSource() != null) {
                            po.setSuccess(true);
                            link.setDestinationCapabilities(toSet(((Source) frame.getSource()).getCapabilities()));
                            if (frame.getMaxMessageSize() != null)
                                link.setMaxMessageSize(frame.getMaxMessageSize().getValue());
                        } else {
                            po.setSuccess(false);
                            po.setException("Invalid destination");
                        }
                    } else {
                        TargetIF target = frame.getTarget();
                        if (target != null) {
                            po.setSuccess(true);
                            if (target instanceof Coordinator)
                                link.setDestinationCapabilities(toSet(((Coordinator) target).getCapabilities()));
                            else {
                                link.setDestinationCapabilities(toSet(((Target) target).getCapabilities()));
                                if (frame.getMaxMessageSize() != null)
                                    link.setMaxMessageSize(frame.getMaxMessageSize().getValue());
                            }
                        } else {
                            po.setSuccess(false);
                            po.setException("Invalid destination");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                remoteHandles.put(link.getRemoteHandle(), link);
                if (link instanceof Producer)
                    ((Producer) link).recover(frame.getUnsettled());
                Semaphore sem = po.getSemaphore();
                if (sem != null)
                    sem.notifySingleWaiter();
            }
        }

        private Set toSet(AMQPArray capabilities) throws IOException {
            Set set = null;
            if (capabilities != null) {
                AMQPType[] t = capabilities.getValue();
                if (t != null && t.length > 0) {
                    set = new HashSet();
                    for (int i = 0; i < t.length; i++)
                        set.add(((AMQPSymbol) t[i]).getValue());
                }
            }
            return set;
        }

        public void visit(FlowFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            if (frame.getNextOutgoingId() != null)
                nextIncomingId = frame.getNextOutgoingId().getValue();
            if (frame.getIncomingWindow() != null)
                remoteIncomingWindow = frame.getIncomingWindow().getValue();
            if (frame.getOutgoingWindow() != null)
                remoteOutgoingWindow = frame.getOutgoingWindow().getValue();
            if (pTracer.isEnabled())
                pTracer.trace(toString(), ", visit=" + frame + ", old remoteIncomingWindow=" + remoteIncomingWindow + ", nextIncomingId=" + nextIncomingId + ", nextOutgoingId=" + nextOutgoingId);
            if (frame.getNextIncomingId() != null)
                remoteIncomingWindow = frame.getNextIncomingId().getValue() + remoteIncomingWindow - nextOutgoingId;
            else
                remoteIncomingWindow = initialOutgoingId + remoteIncomingWindow - nextOutgoingId;
            if (pTracer.isEnabled())
                pTracer.trace(toString(), ", visit=" + frame + ", new remoteIncomingWindow=" + remoteIncomingWindow);
            sendOutboundDeliveries();
            if (frame.getHandle() != null) {
                Link link = (Link) remoteHandles.get(frame.getHandle().getValue());
                if (link != null) {
                    if (link instanceof Producer) {
                        Producer p = (Producer) link;
                        AMQPBoolean drain = frame.getDrain();
                        if (drain != null)
                            p.setDrain(drain.getValue());
                        AMQPUnsignedInt linkCredit = frame.getLinkCredit();
                        if (linkCredit != null)
                            p.setLinkCredit(linkCredit.getValue());
                        AMQPUnsignedInt dcount = frame.getDeliveryCount();
                        if (dcount != null)
                            p.setDeliveryCountRcv(dcount.getValue());
                        boolean echoB = false;
                        AMQPBoolean echo = frame.getEcho();
                        if (echo != null)
                            echoB = echo.getValue();
                        POSendMessage po = p.getWaitingForFlowReleasePO();
                        if (po != null && (p.getLinkCredit() > 0 || p.isDrain())) {
                            doSend(po);
                            p.setWaitingForFlowReleasePO(null);
                        }
                        if (echoB) {
                            FlowFrame flowFrame = new FlowFrame(mySession.getChannel());
                            flowFrame.setHandle(new Handle(p.getHandle()));
                            flowFrame.setAvailable(new AMQPUnsignedInt(p.getAvailable()));
                            flowFrame.setDeliveryCount(new SequenceNo(p.getDeliveryCountSnd()));
                            flowFrame.setDrain(new AMQPBoolean(p.isDrain()));
                            flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
                            flowFrame.setLinkCredit(new AMQPUnsignedInt(p.getLastReceivedLinkCredit()));
                            outboundHandler.send(flowFrame);
                        }
                    }
                }
            }
        }

        private void sendOutboundDeliveries() {
            if (outboundDeliveries.size() > 0) {
                POSendMessage[] pos = (POSendMessage[]) outboundDeliveries.toArray(new POSendMessage[outboundDeliveries.size()]);
                outboundDeliveries.clear();
                for (int i = 0; i < pos.length; i++)
                    doSend(pos[i]);
            }
        }

        public void visit(TransferFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            try {
                Consumer c = (Consumer) remoteHandles.get(frame.getHandle().getValue());
                if (c != null) {
                    AMQPBoolean resumed = frame.getResume();
                    if (resumed != null && resumed.getValue()) {
                        if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame + ", RESUMED!");
                        AMQPBoolean settled = frame.getSettled();
                        if (settled != null && settled.getValue()) {
                            if (pTracer.isEnabled())
                                pTracer.trace(toString(), ", visit=" + frame + ", RESUMED, settle: " + frame.getDeliveryTag());
                            c.getDeliveryMemory().deliverySettled(frame.getDeliveryTag());
                            if (c.getDeliveryMemory().getNumberUnsettled() == 0 && c.getWaitingClosePO() != null) {
                                POObject closePO = c.getWaitingClosePO();
                                c.setWaitingClosePO(null);
                                dispatch(closePO);
                            }
                        }
                        nextIncomingId++;
                        return;
                    }
                    TransferFrame current = c.getCurrentMessage();
                    if (current == null) {
                        current = frame;
                        c.setCurrentMessage(current);
                    } else {
                        current.addMorePayload(frame.getPayload());
                        if (frame.getSettled() != null)
                            current.setSettled(frame.getSettled());
                    }
                    if (frame.getMore() == null || !frame.getMore().getValue()) {
                        c.setCurrentMessage(null);
                        AMQPMessage msg = null;
                        if (current.getMorePayloads() == null)
                            msg = new AMQPMessage(current.getPayload());
                        else {
                            List morePayloads = current.getMorePayloads();
                            byte[][] multiBuffer = new byte[morePayloads.size() + 1][];
                            multiBuffer[0] = current.getPayload();
                            int totalLength = multiBuffer[0].length;
                            for (int i = 0; i < morePayloads.size(); i++) {
                                byte[] b = (byte[]) morePayloads.get(i);
                                multiBuffer[i + 1] = b;
                                totalLength += b.length;
                            }
                            msg = new AMQPMessage(multiBuffer, totalLength);
                        }
                        boolean settled = false;
                        if (current.getSettled() != null)
                            settled = current.getSettled().getValue();
                        msg.setSettled(settled);
                        msg.setDeliveryId(current.getDeliveryId().getValue());
                        msg.setDeliveryTag(current.getDeliveryTag());
                        DeliveryStateIF deliveryStateIF = current.getState();
                        if (deliveryStateIF != null && deliveryStateIF instanceof TransactionalState)
                            msg.setTxnIdIF(((TransactionalState) deliveryStateIF).getTxnId());
                        c.addToCache(msg);
                    }
                } else {
                    DispositionFrame dispoFrame = new DispositionFrame(mySession.getChannel());
                    dispoFrame.setRole(Role.RECEIVER);
                    dispoFrame.setFirst(new DeliveryNumber(frame.getDeliveryId().getValue()));
                    dispoFrame.setSettled(AMQPBoolean.FALSE);
                    Rejected rejected = new Rejected();
                    Error error = new Error();
                    error.setCondition(SessionError.UNATTACHED_HANDLE);
                    rejected.setError(error);
                    dispoFrame.setState(rejected);
                    outboundHandler.send(dispoFrame);
                }
                nextIncomingId++;
                incomingWindow--;
                if (pTracer.isEnabled())
                    pTracer.trace(toString(), ", visit=" + frame + ", incomingWindow=" + incomingWindow);
                if (incomingWindow == 0) {
                    incomingWindow = mySession.getIncomingWindowSize();
                    FlowFrame flowFrame = new FlowFrame(mySession.getChannel());
                    flowFrame.setIncomingWindow(new AMQPUnsignedInt(incomingWindow));
                    flowFrame.setNextIncomingId(new TransferNumber(nextIncomingId));
                    flowFrame.setOutgoingWindow(new AMQPUnsignedInt(outgoingWindow));
                    flowFrame.setNextOutgoingId(new TransferNumber(nextOutgoingId));
                    flowFrame.setDrain(AMQPBoolean.FALSE);
                    flowFrame.setEcho(AMQPBoolean.FALSE);
                    outboundHandler.send(flowFrame);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void visit(DispositionFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            if (frame.getRole().getValue() == Role.SENDER.getValue()) {
                if (frame.getSettled().getValue()) {
                    long from = frame.getFirst().getValue();
                    long to = frame.getFirst().getValue();
                    if (frame.getLast() != null)
                        to = frame.getLast().getValue();
                    settleInbound(from, to, true);
                }
            } else {
                if (frame.getLast() == null)
                    settleOutbound(frame.getFirst().getValue(), frame.getFirst().getValue(), frame.getSettled().getValue(), frame.getState());
                else
                    settleOutbound(frame.getFirst().getValue(), frame.getLast().getValue(), frame.getSettled().getValue(), frame.getState());
                sendOutboundDeliveries();
            }
        }

        public void visit(DetachFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            Link link = (Link) remoteHandles.remove(frame.getHandle().getValue());
            if (link != null) {
                if (link instanceof Producer)
                    removeDeliveries(link, unsettledOutgoingDeliveries);
                else
                    removeDeliveries(link, unsettledIncomingDeliveries);
                handles.set(link.getHandle(), null);
                POObject po = link.getWaitingClosePO();
                Error error = frame.getError();
                if (error != null)
                    link.remoteDetach(frame.getError());
                if (po != null && po.getSemaphore() != null) {
                    if (error != null) {
                        po.setSuccess(false);
                        po.setException(error.getCondition().getValueString() + "/" + error.getDescription().getValue());
                    } else
                        po.setSuccess(true);
                    po.getSemaphore().notifySingleWaiter();
                }
                sendOutboundDeliveries();
            }
        }

        public void visit(EndFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
            remoteEnd = frame;
            checkBothSidesEnd();
        }

        public void visit(CloseFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslMechanismsFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslInitFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslChallengeFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslResponseFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(SaslOutcomeFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public void visit(HeartbeatFrame frame) {
            if (pTracer.isEnabled()) pTracer.trace(toString(), ", visit=" + frame);
        }

        public String toString() {
            return "SessionDispatcherFrameVisitor";
        }
    }

    private class DeliveryMapping {
        DeliveryTag deliveryTag;
        Link link;

        private DeliveryMapping(DeliveryTag deliveryTag, Link link) {
            this.deliveryTag = deliveryTag;
            this.link = link;
        }
    }
}