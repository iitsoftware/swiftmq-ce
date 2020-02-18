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

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Accepted;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressVisitor;
import com.swiftmq.amqp.v100.generated.transactions.coordination.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.*;
import com.swiftmq.amqp.v100.generated.transport.performatives.TransferFrame;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transaction.QueueSenderProvider;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transaction.TransactionRegistry;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.DestinationFactory;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.InboundTransformer;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TemporaryQueueImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.topic.TopicException;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.Destination;
import javax.jms.InvalidSelectorException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

public class TargetLink extends ServerLink
        implements QueueSenderProvider, DestinationFactory, AddressVisitor {
    int rcvSettleMode;
    long linkCredit;
    long flowcontrolDelay = 0;
    long currentLinkCredit = 0;
    long deliveryCount = 0;
    long nmsgs = 0;
    Map transformerList = new HashMap();
    volatile QueueSender sender = null;
    LinkedHashMap unsettledFrames = new LinkedHashMap();
    TransactionRegistry transactionRegistry = null;
    boolean coordinator = false;
    DeliveryStateIF lastDeliveryState = null;
    volatile int numberActiveTx = 0;
    boolean closed = false;
    FlowcontrolTimer flowcontrolTimer = null;
    Entity usage = null;
    Property propLinkCredit = null;
    Property propDeliveryCount = null;
    Property propMessagesReceived = null;
    Property propUnsettledTransfers = null;
    Destination lastDestination = null;
    TransferFrame currentMessage = null;
    DeliveryNumber lastDeliveryId = null;
    Map remoteUnsettled = null;
    String queueName = null;

    public TargetLink(SwiftletContext ctx, SessionHandler mySessionHandler, String name, int rcvSettleMode) {
        super(ctx, mySessionHandler, name);
        this.rcvSettleMode = rcvSettleMode;
        transactionRegistry = mySessionHandler.getTransactionRegistry();
        linkCredit = ((Long) mySessionHandler.getVersionedConnection().getConnectionTemplate().getProperty("target-link-credit").getValue()).longValue();
        currentLinkCredit = linkCredit;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/created");
    }

    public void setUsage(Entity usage) {
        this.usage = usage;
        if (usage != null) {
            propLinkCredit = usage.getProperty("link-credit");
            propDeliveryCount = usage.getProperty("delivery-count");
            propMessagesReceived = usage.getProperty("messages-received");
            propUnsettledTransfers = usage.getProperty("unsettled-transfers");
        }
    }

    public void fillUsage() {
        if (usage != null) {
            try {
                if (((Long) propLinkCredit.getValue()).longValue() != currentLinkCredit)
                    propLinkCredit.setValue(new Long(currentLinkCredit));
                if (((Long) propDeliveryCount.getValue()).longValue() != deliveryCount)
                    propDeliveryCount.setValue(new Long(deliveryCount));
                if (((Long) propMessagesReceived.getValue()).longValue() != nmsgs)
                    propMessagesReceived.setValue(new Long(nmsgs));
                int size = unsettledFrames.size();
                if (((Integer) propUnsettledTransfers.getValue()).longValue() != size)
                    propUnsettledTransfers.setValue(new Integer(size));
            } catch (Exception e) {
            }
        }
    }

    public void setLocalAddress(AddressIF localAddress) {
        super.setLocalAddress(localAddress);
        if (usage != null && localAddress != null)
            localAddress.accept(new AddressVisitor() {
                public void visit(AddressString addressString) {
                    try {
                        usage.getProperty("local-address").setValue(addressString.getValue());
                    } catch (Exception e) {
                    }
                }
            });
    }

    protected void setRemoteAddress(AddressIF remoteAddress) {
        super.setRemoteAddress(remoteAddress);
        if (usage != null && remoteAddress != null)
            remoteAddress.accept(new AddressVisitor() {
                public void visit(AddressString addressString) {
                    try {
                        usage.getProperty("remote-address").setValue(addressString.getValue());
                    } catch (Exception e) {
                    }
                }
            });
    }

    public long getLinkCredit() {
        return linkCredit;
    }

    public void setFlowcontrolDelay(long flowcontrolDelay) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/setFlowcontrolDelay, flowcontrolDelay=" + flowcontrolDelay);
        this.flowcontrolDelay = flowcontrolDelay;
    }

    public long getFlowcontrolDelay() {
        return flowcontrolDelay;
    }

    public FlowcontrolTimer getFlowcontrolTimer() {
        return flowcontrolTimer;
    }

    public void setFlowcontrolTimer(FlowcontrolTimer flowcontrolTimer) {
        this.flowcontrolTimer = flowcontrolTimer;
    }

    public void setDeliveryCount(long deliveryCount) {
        this.deliveryCount = deliveryCount;
    }

    public long getDeliveryCount() {
        return deliveryCount;
    }

    public int getRcvSettleMode() {
        return rcvSettleMode;
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public void setCoordinator(boolean coordinator) {
        this.coordinator = coordinator;
    }

    public void setRemoteUnsettled(Map remoteUnsettled) {
        this.remoteUnsettled = remoteUnsettled;
    }

    public AMQPArray getOfferedCapabilitiesArray() {
        try {
            if (coordinator)
                return new AMQPArray(AMQPTypeDecoder.SYM8, new AMQPType[]{TxnCapability.LOCAL_TRANSACTIONS, TxnCapability.MULTI_TXNS_PER_SSN});
            return null;
        } catch (IOException e) {
        }
        return null;
    }

    public void visit(AddressString addressString) {
        try {
            String name = SwiftUtilities.extractAMQPName(addressString.getValue());
            if (ctx.queueManager.isQueueRunning(name)) {
                if (name.indexOf('@') == -1)
                    name += "@" + SwiftletManager.getInstance().getRouterName();
                if (ctx.queueManager.isTemporaryQueue(name))
                    lastDestination = new TemporaryQueueImpl(name, null);
                else
                    lastDestination = new QueueImpl(name);
            } else {
                if (ctx.topicManager.isTopicDefined(name))
                    lastDestination = new TopicImpl(ctx.topicManager.getQueueForTopic(name), name);
                else
                    lastDestination = null;
            }
        } catch (MalformedURLException e) {
            lastDestination = null;
        }
    }

    public Destination create(AddressIF addressIF) {
        addressIF.accept(this);
        return lastDestination;
    }

    public void verifyLocalAddress() throws AuthenticationException, QueueException, TopicException, InvalidSelectorException {
        if (coordinator)
            return;
        if (!dynamic)
            super.verifyLocalAddress();
        if (sender == null) {
            if (dynamic) {
                queueName = ctx.queueManager.createTemporaryQueue();
                sender = ctx.queueManager.createQueueSender(queueName, mySessionHandler.getVersionedConnection().getActiveLogin());
            } else {
                if (isQueue)
                    sender = ctx.queueManager.createQueueSender(getLocalAddress().getValueString(), mySessionHandler.getVersionedConnection().getActiveLogin());
                else
                    sender = ctx.queueManager.createQueueSender(ctx.topicManager.getQueueForTopic(getLocalAddress().getValueString()), mySessionHandler.getVersionedConnection().getActiveLogin());
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/verifyLocalAddress, localDestination=" + localDestination);
    }

    private InboundTransformer getTransformer(long messageFormat) throws EndWithErrorException {
        InboundTransformer tf = (InboundTransformer) transformerList.get(messageFormat);
        if (tf == null) {
            try {
                tf = ctx.transformerFactory.createInboundTransformer(messageFormat, getLocalAddress().getValueString());
            } catch (Exception e) {
                throw new LinkEndException(this, AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
            }
            if (tf == null)
                throw new LinkEndException(this, AmqpError.INTERNAL_ERROR, new AMQPString("No inbound transformer found for messageFormat=" + messageFormat + ", localAddress=" + getLocalAddress().getValueString()));
            transformerList.put(messageFormat, tf);
        }
        return tf;
    }

    public DeliveryStateIF getLastDeliveryState() {
        if (coordinator) {
            DeliveryStateIF ds = lastDeliveryState;
            lastDeliveryState = null;
            return ds;
        }
        return new Accepted();
    }

    public DeliveryNumber getLastDeliveryId() {
        return lastDeliveryId;
    }

    public long getCurrentMessageSize() {
        return currentMessage == null ? 0 : currentMessage.getPayloadLength();
    }

    public void abortCurrentMessage() {
        currentMessage = null;
    }

    public QueueSender getQueueSender() {
        return sender;
    }

    public String getQueueName() {
        return queueName;
    }

    public void increaseActiveTransactions() {
        numberActiveTx++;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/increaseActiveTransactions, numberActiveTx=" + numberActiveTx);
    }

    public void decreaseActiveTransactions() {
        numberActiveTx--;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/decreaseActiveTransactions, numberActiveTx=" + numberActiveTx);
    }

    public void closeResource() {
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close, sender=" + sender + ", numberActiveTx=" + numberActiveTx);
            if (closed && sender != null && numberActiveTx == 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close, closing sender");
                sender.close();
                sender = null;
            }
        } catch (QueueException e) {
        }
    }

    private void handleDeclare(Declare declare) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/handleDeclare, declare=" + declare);
        Declared declared = new Declared();
        declared.setTxnId(transactionRegistry.createTxId());
        lastDeliveryState = declared;
    }

    private void handleDischarge(Discharge discharge) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/handleDischarge, discharge=" + discharge);
        transactionRegistry.discharge(discharge.getTxnId(), discharge.getFail() == null ? false : discharge.getFail().getValue());
    }

    private void handleTransactionRequest(TransferFrame frame) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/handleTransactionRequest, frame=" + frame);
        AMQPMessage msg = null;
        try {
            msg = new AMQPMessage(frame.getPayload());
        } catch (Exception e) {
            e.printStackTrace();
            new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Exception during decode of a message: " + e));
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/handleTransactionRequest, msg=" + msg);
        if (msg.getAmqpValue() == null)
            throw new SessionEndException(AmqpError.NOT_FOUND, new AMQPString("Missing amqp-value message body in transaction request!"));
        AMQPType bare = msg.getAmqpValue().getValue();
        AMQPDescribedConstructor constructor = bare.getConstructor();
        if (constructor == null)
            throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Missing constructor: " + bare));
        if (!AMQPTypeDecoder.isList(constructor.getFormatCode()))
            throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Message body is not of a list type, code=0x" + Integer.toHexString(bare.getCode()) + ", bare=" + bare));
        AMQPType descriptor = constructor.getDescriptor();
        int code = descriptor.getCode();
        try {
            if (AMQPTypeDecoder.isULong(code)) {
                long type = ((AMQPUnsignedLong) descriptor).getValue();
                if (type == Declare.DESCRIPTOR_CODE)
                    handleDeclare(new Declare(((AMQPList) bare).getValue()));
                else if (type == Discharge.DESCRIPTOR_CODE)
                    handleDischarge(new Discharge(((AMQPList) bare).getValue()));
                else
                    throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Invalid descriptor type: " + type + ", bare=" + bare));
            } else if (AMQPTypeDecoder.isSymbol(code)) {
                String type = ((AMQPSymbol) descriptor).getValue();
                if (type.equals(Declare.DESCRIPTOR_NAME))
                    handleDeclare(new Declare(((AMQPList) bare).getValue()));
                else if (type.equals(Discharge.DESCRIPTOR_NAME))
                    handleDischarge(new Discharge(((AMQPList) bare).getValue()));
                else
                    throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Invalid descriptor type: " + type + ", bare=" + bare));
            } else
                throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString("Invalid type of constructor descriptor (actual type=" + code + ", expected=symbold or ulong), bare= " + bare));
        } catch (Exception e) {
            e.printStackTrace();
            throw new SessionEndException(AmqpError.DECODE_ERROR, new AMQPString(e.toString()));
        }
    }

    private void transformAndStore(TransferFrame frame) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/transformAndStore, frame=" + frame);
        if (mySessionHandler.maxMessageSize > 0 && frame.getPayloadLength() > mySessionHandler.maxMessageSize)
            throw new LinkEndException(this, LinkError.MESSAGE_SIZE_EXCEEDED, new AMQPString("Message size (" + frame.getPayloadLength() + ") > max message size (" + mySessionHandler.maxMessageSize));
        if (coordinator)
            handleTransactionRequest(frame);
        else {
            try {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/transformAndStore, localDestination=" + localDestination);
                long payloadLength = frame.getPayloadLength();
                MessageImpl msg = getTransformer(frame.getMessageFormat().getValue()).transform(frame, this);
                if (msg.getJMSDestination() == null)
                    msg.setJMSDestination(getLocalDestination());
                msg.setStringProperty(MessageImpl.PROP_CLIENT_ID, mySessionHandler.getVersionedConnection().getActiveLogin().getClientId());
                if (remoteUnsettled != null) {
                    DeliveryTag deliveryTag = frame.getDeliveryTag();
                    if (remoteUnsettled.remove(deliveryTag) != null) {
                        msg.setBooleanProperty(MessageImpl.PROP_DOUBT_DUPLICATE, true);
                        if (remoteUnsettled.size() == 0)
                            remoteUnsettled = null;
                    }
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/transformAndStore, msg=" + msg);
                DeliveryStateIF state = frame.getState();
                if (state != null && state instanceof TransactionalState) {
                    TransactionalState txState = (TransactionalState) state;
                    TxnIdIF txnId = txState.getTxnId();
                    if (txnId != null) {
                        transactionRegistry.addToTransaction(txnId, name, msg, this);
                    } else
                        throw new SessionEndException(TransactionError.UNKNOWN_ID, new AMQPString("Missing TxnId in TransactionalState: " + txState));
                } else {
                    QueuePushTransaction tx = sender.createTransaction();
                    tx.putMessage(msg);
                    tx.commit();
                    setFlowcontrolDelay(tx.getFlowControlDelay());
                }
            } catch (Exception e) {
                throw new SessionEndException(AmqpError.INTERNAL_ERROR, new AMQPString("Exception during transformAndStore: " + e));
            }
        }
    }

    public boolean addTransferFrame(TransferFrame frame) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addTransferFrame, frame=" + frame);
        if (currentMessage == null)
            currentMessage = frame;
        else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addTransferFrame, frame=" + frame + ", add payload to currentMessage");
            currentMessage.addMorePayload(frame.getPayload());
            if (frame.getSettled() != null)
                currentMessage.setSettled(frame.getSettled());
        }
        if (frame.getMore() != null && frame.getMore().getValue())
            return false;
        boolean settled = false;
        if (currentMessage.getSettled() != null)
            settled = currentMessage.getSettled().getValue();
        lastDeliveryId = currentMessage.getDeliveryId();
        DeliveryStateIF state = currentMessage.getState();
        if (settled || rcvSettleMode == ReceiverSettleMode.FIRST.getValue() || state != null && state instanceof TransactionalState)
            transformAndStore(currentMessage);
        else
            unsettledFrames.put(currentMessage.getDeliveryId().getValue(), currentMessage);
        deliveryCount++;
        currentLinkCredit--;
        nmsgs++;
        boolean needMore = currentLinkCredit == 0;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/addTransferFrame, frame=" + frame + ", currentLinkCredit=" + currentLinkCredit + ", needMore=" + needMore);
        if (needMore)
            currentLinkCredit = linkCredit;
        currentMessage = null;
        return needMore;
    }

    public void settle(long deliveryId, DeliveryStateIF deliveryState) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/settle, deliveryId=" + deliveryId);
        TransferFrame frame = (TransferFrame) unsettledFrames.remove(deliveryId);
        if (frame != null)
            transformAndStore(frame);
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close");
        closed = true;
        // Settle all unsettled incoming transfers in the order of delivery (on redelivery duplicates will be discarded by dup detection)
        if (unsettledFrames.size() > 0) {
            Collection values = unsettledFrames.values();
            for (Iterator iter = values.iterator(); iter.hasNext(); ) {
                try {
                    transformAndStore((TransferFrame) iter.next());
                } catch (EndWithErrorException e) {
                }
            }
        }
        if (flowcontrolTimer != null) {
            ctx.timerSwiftlet.removeTimerListener(flowcontrolTimer);
            flowcontrolTimer = null;
        }
        try {
            if (dynamic)
                ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (QueueException e) {
        }
        unsettledFrames.clear();
        transformerList.clear();
        closeResource();
        super.close();
    }

    public String toString() {
        return mySessionHandler.toString() + "/TargetLink, name=" + name + ", localAddress=" + (localAddress == null ? "null" : localAddress.getValueString());
    }
}
