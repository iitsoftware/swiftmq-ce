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

import com.swiftmq.amqp.v100.generated.messaging.addressing.TerminusDurability;
import com.swiftmq.amqp.v100.generated.messaging.addressing.TerminusExpiryPolicy;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.transactions.coordination.Declared;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TransactionalState;
import com.swiftmq.amqp.v100.generated.transactions.coordination.TxnIdIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.AmqpError;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transaction.QueueReceiverProvider;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transaction.TransactionRegistry;
import com.swiftmq.impl.amqp.amqp.v01_00_00.transformer.OutboundTransformer;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.auth.AuthenticationException;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.topic.TopicException;
import com.swiftmq.tools.util.DataByteArrayInputStream;

import javax.jms.InvalidSelectorException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SourceLink extends ServerLink
        implements QueueReceiverProvider {
    AMQPArray supportedOutcomes = null;
    OutcomeIF defaultOutcome = new Released();
    int sndSettleMode;
    long linkCredit = 0;
    long nmsgs = 0;
    long deliveryCountSnd = 0;
    long deliveryCountRcv = 0;
    TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.LINK_DETACH;
    TerminusDurability durability = TerminusDurability.NONE;
    boolean noLocal = false;
    String messageSelector = null;
    boolean drain = false;
    boolean flowAfterDrainRequired = false;
    int subscriberId = -1;
    String queueName = null;
    QueueReceiver receiver = null;
    QueuePullTransaction readTransaction = null;
    SourceMessageProcessor messageProcessor = null;
    OutboundTransformer transformer = null;
    volatile int numberActiveTx = 0;
    Map unsettled = new HashMap();
    TransactionRegistry transactionRegistry;
    TxnIdIF currentTx = null;
    Entity usage = null;
    Property propLinkCredit = null;
    Property propDeliveryCountSnd = null;
    Property propDeliveryCountRcv = null;
    Property propMessagesSent = null;
    Property propUnsettledTransfers = null;
    Property propDrain = null;
    Property propNoLocal = null;
    Property propSelector = null;
    Map remoteUnsettled = null;

    public SourceLink(SwiftletContext ctx, SessionHandler mySessionHandler, String name, int sndSettleMode) {
        super(ctx, mySessionHandler, name);
        this.sndSettleMode = sndSettleMode;
        transactionRegistry = mySessionHandler.getTransactionRegistry();
        try {
            supportedOutcomes = new AMQPArray(AMQPTypeDecoder.SYM8, new AMQPType[]{new AMQPSymbol(Accepted.DESCRIPTOR_NAME), new AMQPSymbol(Rejected.DESCRIPTOR_NAME), new AMQPSymbol(Released.DESCRIPTOR_NAME), new AMQPSymbol(Modified.DESCRIPTOR_NAME)});
        } catch (IOException e) {
        }
    }

    public void setUsage(Entity usage) {
        this.usage = usage;
        if (usage != null) {
            propLinkCredit = usage.getProperty("link-credit");
            propDeliveryCountSnd = usage.getProperty("delivery-count-snd");
            propDeliveryCountRcv = usage.getProperty("delivery-count-rcv");
            propMessagesSent = usage.getProperty("messages-sent");
            propUnsettledTransfers = usage.getProperty("unsettled-transfers");
            propNoLocal = usage.getProperty("nolocal");
            propSelector = usage.getProperty("selector");
            propDrain = usage.getProperty("drain");
        }
    }

    public void fillUsage() {
        if (usage != null) {
            try {
                setIfNotEqual(propLinkCredit, getLinkCredit());
                setIfNotEqual(propDeliveryCountSnd, deliveryCountSnd);
                setIfNotEqual(propDeliveryCountRcv, deliveryCountRcv);
                setIfNotEqual(propMessagesSent, nmsgs);
                setIfNotEqual(propUnsettledTransfers, unsettled.size());
                setIfNotEqual(propDrain, drain);
            } catch (Exception e) {
                // Handle exception appropriately
            }
        }
    }

    private void setIfNotEqual(Property property, long value) throws Exception {
        if ((Long) property.getValue() != value) {
            property.setValue(value);
        }
    }

    private void setIfNotEqual(Property property, int value) throws Exception {
        if ((Integer) property.getValue() != value) {
            property.setValue(value);
        }
    }

    private void setIfNotEqual(Property property, boolean value) throws Exception {
        if ((Boolean) property.getValue() != value) {
            property.setValue(value);
        }
    }

    public void setLocalAddress(AddressIF localAddress) {
        super.setLocalAddress(localAddress);
        if (usage != null && localAddress != null)
            localAddress.accept(addressString -> {
                try {
                    usage.getProperty("local-address").setValue(addressString.getValue());
                } catch (Exception e) {
                }
            });
    }

    protected void setRemoteAddress(AddressIF remoteAddress) {
        super.setRemoteAddress(remoteAddress);
        if (usage != null && remoteAddress != null)
            remoteAddress.accept(addressString -> {
                try {
                    usage.getProperty("remote-address").setValue(addressString.getValue());
                } catch (Exception e) {
                }
            });
    }

    public AMQPArray getSupportedOutcomes() {
        return supportedOutcomes;
    }

    public void setDefaultOutcome(OutcomeIF defaultOutcome) {
        defaultOutcome.accept(new OutcomeVisitorAdapter() {
            public void visit(Accepted impl) {
                SourceLink.this.defaultOutcome = impl;
            }

            public void visit(Rejected impl) {
                SourceLink.this.defaultOutcome = impl;
            }

            public void visit(Released impl) {
                SourceLink.this.defaultOutcome = impl;
            }
        });
    }

    public OutcomeIF getDefaultOutcome() {
        return defaultOutcome;
    }

    public int getSndSettleMode() {
        return sndSettleMode;
    }

    public long getLinkCredit() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + ", getLinkCredit, deliveryCountRcv=" + deliveryCountRcv + ", linkCredit=" + linkCredit + ", deliveryCountSnd=" + deliveryCountSnd);
        return deliveryCountRcv + linkCredit - deliveryCountSnd;
    }

    public long getLastReceivedLinkCredit() {
        return linkCredit;
    }

    public void setLinkCredit(long linkCredit) {
        this.linkCredit = linkCredit;
    }

    public TerminusExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    public void setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
    }

    public TerminusDurability getDurability() {
        return durability;
    }

    public void setDurability(TerminusDurability durability) {
        this.durability = durability;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
        try {
            if (usage != null)
                propNoLocal.setValue(noLocal);
        } catch (Exception e) {
        }
    }

    public void setMessageSelector(String messageSelector) {
        this.messageSelector = messageSelector;
        try {
            if (usage != null && messageSelector != null)
                propSelector.setValue(messageSelector);
        } catch (Exception e) {
        }
    }

    public void setRemoteUnsettled(Map remoteUnsettled) {
        this.remoteUnsettled = remoteUnsettled;
    }

    public String getQueueName() {
        return queueName;
    }

    public TxnIdIF getCurrentTx() {
        return currentTx;
    }

    public void setCurrentTx(TxnIdIF currentTx) {
        this.currentTx = currentTx;
    }

    public void startMessageProcessor() throws QueueException {
        if (readTransaction == null)
            readTransaction = receiver.createTransaction(false);
        if (messageProcessor == null) {
            messageProcessor = new SourceMessageProcessor(ctx, this);
            readTransaction.registerMessageProcessor(messageProcessor);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/startMessageProcessor, mp=" + messageProcessor);
        }
    }

    public void startMessageProcessor(SourceMessageProcessor messageProcessor) throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/startMessageProcessor, mp=" + messageProcessor);
        this.messageProcessor = messageProcessor;
        readTransaction.registerMessageProcessor(messageProcessor);
    }

    public void clearMessageProcessor() {
        messageProcessor = null;
    }

    public boolean isMessageProcessorRunning() {
        return messageProcessor != null;
    }

    public void stopMessageProcessor() throws QueueException {
        if (messageProcessor != null && readTransaction != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/stopMessageProcessor");
            messageProcessor.setStopped(true);
            readTransaction.unregisterMessageProcessor(messageProcessor);
            readTransaction.rollback();
            readTransaction = null;
            messageProcessor = null;
        }
    }

    public OutboundTransformer getTransformer() throws EndWithErrorException {
        // Creates always AMQPMessage with message format 0 (there can be messages in the queues, originated from JMS clients without a message format property)
        try {
            if (transformer == null)
                transformer = ctx.transformerFactory.createOutboundTransformer(0, dynamic ? queueName : localAddress.getValueString());
        } catch (Exception e) {
            throw new LinkEndException(this, AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
        }
        return transformer;
    }

    public long getAvailable() {
        // number messages waiting in queue
        if (receiver != null) {
            try {
                return receiver.getNumberQueueMessages();
            } catch (QueueException e) {
            }
        }
        return 0;
    }

    public long getDeliveryCountSnd() {
        return deliveryCountSnd;
    }

    public void setDeliveryCountRcv(long deliveryCountRcv) {
        this.deliveryCountRcv = deliveryCountRcv;
    }

    public void incDeliveryCountSnd() {
        deliveryCountSnd++;
        nmsgs++;
    }

    public boolean isDrain() {
        return drain;
    }

    public void setDrain(boolean drain) {
        flowAfterDrainRequired = drain;
        this.drain = drain;
    }

    public boolean isFlowAfterDrainRequired() {
        return flowAfterDrainRequired;
    }

    public void setFlowAfterDrainRequired(boolean flowAfterDrainRequired) {
        this.flowAfterDrainRequired = flowAfterDrainRequired;
    }

    public void advanceDeliveryCount() {
        deliveryCountSnd += getLinkCredit();
    }


    public void verifyLocalAddress() throws AuthenticationException, QueueException, TopicException, InvalidSelectorException {
        if (!dynamic) {
            // This is for the case of reconnecting to a Durable Subscriber without specifying a Source in the attach frame.
            if (localAddress == null) {
                String topicName = ctx.topicManager.getDurableTopicName(getName(), mySessionHandler.getVersionedConnection().getActiveLogin());
                if (topicName != null)
                    localAddress = new AddressString(topicName);
                durability = TerminusDurability.CONFIGURATION;
            }
            super.verifyLocalAddress();
        }
        if (receiver == null) {
            MessageSelector msel = null;
            if (messageSelector != null) {
                msel = new MessageSelector(messageSelector);
                msel.compile();
            }
            if (dynamic) {
                expiryPolicy = TerminusExpiryPolicy.LINK_DETACH;
                durability = TerminusDurability.NONE;
//        sndSettleMode = SenderSettleMode.SETTLED.getValue();       // SETTLED is only possible with temp queues because bulk mode in message proc do not delete from persistent store
                queueName = ctx.queueManager.createTemporaryQueue();
                receiver = ctx.queueManager.createQueueReceiver(queueName, mySessionHandler.getVersionedConnection().getActiveLogin(), msel);
            } else {
                if (isQueue) {
                    expiryPolicy = TerminusExpiryPolicy.LINK_DETACH;
                    durability = TerminusDurability.CONFIGURATION;
//          sndSettleMode = SenderSettleMode.UNSETTLED.getValue();
                    receiver = ctx.queueManager.createQueueReceiver(getLocalAddress().getValueString(), mySessionHandler.getVersionedConnection().getActiveLogin(), msel);
                } else {
                    if (durability.getValue() == TerminusDurability.CONFIGURATION.getValue() || durability.getValue() == TerminusDurability.UNSETTLED_STATE.getValue()) {
                        if (!expiryPolicy.getValue().equals(TerminusExpiryPolicy.LINK_DETACH.getValue()))
                            expiryPolicy = TerminusExpiryPolicy.NEVER;
                        durability = TerminusDurability.CONFIGURATION;
//            sndSettleMode = SenderSettleMode.UNSETTLED.getValue();
                        queueName = ctx.topicManager.subscribeDurable(name, (TopicImpl) getLocalDestination(), msel, noLocal, mySessionHandler.getVersionedConnection().getActiveLogin());
                    } else {
                        expiryPolicy = TerminusExpiryPolicy.LINK_DETACH;
                        queueName = ctx.queueManager.createTemporaryQueue();
//            sndSettleMode = SenderSettleMode.SETTLED.getValue(); // SETTLED is only possible with temp queues because bulk mode in message proc do not delete from persistent store
                        subscriberId = ctx.topicManager.subscribe((TopicImpl) localDestination, msel, noLocal, queueName, mySessionHandler.getVersionedConnection().getActiveLogin());
                    }
                    receiver = ctx.queueManager.createQueueReceiver(queueName, mySessionHandler.getVersionedConnection().getActiveLogin(), null);
                }
            }
        }
        if (remoteUnsettled != null) {
            final AbstractQueue aq = ctx.queueManager.getQueueForInternalUse(receiver.getQueueName());
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/recovery, abstractQueue=" + aq);
            DataByteArrayInputStream dbis = new DataByteArrayInputStream();
            for (Object o : remoteUnsettled.entrySet()) {
                Map.Entry entry = (Map.Entry) o;
                AMQPBinary deliveryTag = (AMQPBinary) entry.getKey();
                DeliveryStateIF deliveryStateIF;
                try {
                    deliveryStateIF = DeliveryStateFactory.create((AMQPList) entry.getValue());
                } catch (Exception e) {
                    throw new QueueException("Unable to create delivery tag");
                }
                final MessageIndex messageIndex = new MessageIndex();
                dbis.setBuffer(deliveryTag.getValue());
                try {
                    messageIndex.readContent(dbis);
                } catch (IOException e) {
                    throw new QueueException("Unable to convert delivery tag into a message index");
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/recovery, messageIndex=" + messageIndex + ", deliveryStateIF=" + deliveryStateIF);
                if (deliveryStateIF != null) {
                    deliveryStateIF.accept(new DeliveryStateVisitorAdapter() {
                        public void visit(Accepted accepted) {
                            try {
                                if (aq != null) {
                                    MessageIndex indexEntry = aq.getIndexEntry(messageIndex);
                                    if (indexEntry != null) {
                                        aq.removeMessageByIndex(indexEntry);
                                        if (ctx.traceSpace.enabled)
                                            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/recovery, messageIndex=" + indexEntry + ", removed");
                                    } else {
                                        if (ctx.traceSpace.enabled)
                                            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/recovery, messageIndex=" + messageIndex + ", NOT FOUND!");
                                    }
                                }
                            } catch (QueueException e) {
                                e.printStackTrace();
                                if (ctx.traceSpace.enabled)
                                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/recovery, messageIndex=" + messageIndex + ", exception=" + e);
                            }
                        }
                    });
                }
            }
        }
    }

    public QueueReceiver getQueueReceiver() {
        return receiver;
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
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close, receiver=" + receiver + ", numberActiveTx=" + numberActiveTx + ", currentTx=" + currentTx);
            currentTx = null;
            if (closed && receiver != null && numberActiveTx == 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/close, closing receiver");
                receiver.close();
                receiver = null;
            }
        } catch (QueueException e) {
        }
    }

    public void addUnsettled(long deliveryId, MessageIndex messageIndex, long size) {
        addUnsettled(deliveryId, messageIndex);
    }

    public void addUnsettled(long deliveryId, MessageIndex messageIndex) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", addUnsettled, deliveryId=" + deliveryId + ", messageIndex=" + messageIndex);
        unsettled.put(deliveryId, messageIndex);
    }

    public void settle(long deliveryId, DeliveryStateIF deliveryState) throws EndWithErrorException {
        final ExceptionHolder exceptionHolder = new ExceptionHolder();
        final MessageIndex messageIndex = (MessageIndex) unsettled.remove(deliveryId);
        if (messageIndex != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", settle, deliveryId=" + deliveryId + ", deliveryState=" + deliveryState + ", messageIndex=" + messageIndex);
            if (deliveryState == null)
                deliveryState = (DeliveryStateIF) defaultOutcome;
            deliveryState.accept(new DeliveryStateVisitor() {
                public void visit(Received received) {
                    exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INVALID_FIELD, new AMQPString("Invalid delivery state: " + received));
                }

                public void visit(Accepted accepted) {
                    try {
                        readTransaction.acknowledgeMessage(messageIndex);
                    } catch (QueueException e) {
                        exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
                    }
                }

                public void visit(Rejected rejected) {
                    try {
                        releaseMessage(messageIndex, true);
                    } catch (EndWithErrorException e) {
                        exceptionHolder.endWithErrorException = e;
                    }
                }

                public void visit(Released released) {
                    try {
                        releaseMessage(messageIndex, true);
                    } catch (EndWithErrorException e) {
                        exceptionHolder.endWithErrorException = e;
                    }
                }

                public void visit(Modified modified) {
                    try {
                        releaseMessage(messageIndex, modified.getDeliveryFailed() != null && modified.getDeliveryFailed().getValue());
                    } catch (EndWithErrorException e) {
                        exceptionHolder.endWithErrorException = e;
                    }
                }

                public void visit(Declared declared) {
                    exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INVALID_FIELD, new AMQPString("Invalid delivery state: " + declared));
                }

                public void visit(TransactionalState txState) {
                    OutcomeIF outcomeIF = txState.getOutcome();
                    outcomeIF.accept(new OutcomeVisitor() {
                        public void visit(Accepted accepted) {
                            try {
                                readTransaction.acknowledgeMessage(messageIndex);
                            } catch (QueueException e) {
                                exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
                            }
                        }

                        public void visit(Rejected rejected) {
                            try {
                                releaseMessage(messageIndex, true);
                            } catch (EndWithErrorException e) {
                                exceptionHolder.endWithErrorException = e;
                            }
                        }

                        public void visit(Released released) {
                            try {
                                releaseMessage(messageIndex, true);
                            } catch (EndWithErrorException e) {
                                exceptionHolder.endWithErrorException = e;
                            }
                        }

                        public void visit(Modified modified) {
                            exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INVALID_FIELD, new AMQPString("Invalid transactional outcome: " + modified));
                        }

                        public void visit(Declared declared) {
                            exceptionHolder.endWithErrorException = new LinkEndException(SourceLink.this, AmqpError.INVALID_FIELD, new AMQPString("Invalid transactional outcome: " + declared));
                        }
                    });
                }
            });
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", settle, deliveryId=" + deliveryId + " NOT FOUND!");
            exceptionHolder.endWithErrorException = new LinkEndException(this, AmqpError.INVALID_FIELD, new AMQPString("DeliveryId " + deliveryId + " NOT FOUND!"));
        }
        if (exceptionHolder.endWithErrorException != null)
            throw exceptionHolder.endWithErrorException;
    }

    private void releaseMessage(MessageIndex messageIndex, boolean incDeliveryCount) throws EndWithErrorException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", releaseMessage, messageIndex=" + messageIndex + ", incDeliveryCount=" + incDeliveryCount);
        try {
            QueuePullTransaction t = receiver.createTransaction(incDeliveryCount);
            t.moveToTransaction(messageIndex, readTransaction);
            t.rollback();
        } catch (QueueException e) {
            throw new LinkEndException(this, AmqpError.INTERNAL_ERROR, new AMQPString(e.toString()));
        }
    }

    public void autoack(MessageIndex messageIndex) throws QueueException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + "/ack, messageIndex=" + messageIndex);
        QueuePullTransaction t = receiver.createTransaction(true);
        t.moveToTransaction(messageIndex, readTransaction);
        t.commit();
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close ...");
        super.close();
        try {
            Map cloned = (Map) ((HashMap) unsettled).clone();
            for (Iterator iter = cloned.keySet().iterator(); iter.hasNext(); )
                settle(((Long) iter.next()).longValue(), (DeliveryStateIF) defaultOutcome);
            cloned.clear();
            stopMessageProcessor();
            closeResource();
            if (dynamic)
                ctx.queueManager.deleteTemporaryQueue(queueName);
            else {
                if (!isQueue) {
                    if (subscriberId != -1) {
                        ctx.topicManager.unsubscribe(subscriberId);
                        ctx.queueManager.deleteTemporaryQueue(queueName);
                    } else {
                        if (expiryPolicy.getValue().equals(TerminusExpiryPolicy.LINK_DETACH.getValue()))
                            ctx.topicManager.deleteDurable(name, mySessionHandler.getVersionedConnection().getActiveLogin());
                    }
                }
            }
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", close done");
    }

    public String toString() {
        return mySessionHandler.toString() + "/SourceLink, name=" + name + ", localAddress=" + (localAddress == null ? "null" : localAddress.getValueString() + ", sndSettleMode=" + sndSettleMode);
    }

    private class ExceptionHolder {
        EndWithErrorException endWithErrorException = null;
    }
}
