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

import com.swiftmq.amqp.v100.client.po.POSendMessage;
import com.swiftmq.amqp.v100.client.po.POSendResumedTransfer;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.Accepted;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateFactory;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateIF;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.DeliveryStateVisitorAdapter;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressString;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Header;
import com.swiftmq.amqp.v100.generated.messaging.message_format.MessageIdString;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.generated.transport.definitions.Milliseconds;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.util.IdGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * <p>A message producer, created from a session.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class Producer extends Link {
    String target;
    boolean defaultPersistent = true;
    int defaultPriority = 5;
    long defaultTtl = -1;
    volatile long linkCredit = 0;
    volatile long deliveryCountSnd = 0;
    volatile long deliveryCountRcv = 0;
    volatile boolean drain = false;
    String uniqueId = IdGenerator.getInstance().nextId('/');
    long msgId = 0;
    boolean transactionController = false;
    POSendMessage waitingForFlowReleasePO = null;

    protected Producer(Session mySession, String target, String name, int qoS, DeliveryMemory deliveryMemory) {
        super(mySession, name, qoS, deliveryMemory);
        this.target = target;
    }

    protected boolean isTransactionController() {
        return transactionController;
    }

    protected void setTransactionController(boolean transactionController) {
        this.transactionController = transactionController;
    }

    /**
     * Returns the target (e.g. queue/topic name)
     *
     * @return target
     */
    public String getTarget() {
        return target;
    }

    protected long getLinkCredit() {
        return deliveryCountRcv + linkCredit - deliveryCountSnd;
    }

    protected long getLastReceivedLinkCredit() {
        return linkCredit;
    }

    protected void setLinkCredit(long linkCredit) {
        this.linkCredit = linkCredit;
    }

    protected long getAvailable() {
        return getWaitingPO() != null ? 1 : 0;
    }

    protected long getDeliveryCountSnd() {
        return deliveryCountSnd;
    }

    protected void setDeliveryCountRcv(long deliveryCountRcv) {
        this.deliveryCountRcv = deliveryCountRcv;
    }

    protected void incDeliveryCountSnd() {
        deliveryCountSnd++;
    }

    protected boolean isDrain() {
        return drain;
    }

    protected void setDrain(boolean drain) {
        this.drain = drain;
        if (drain) {
            deliveryCountSnd += linkCredit;
            linkCredit = 0;
        }
    }

    public POSendMessage getWaitingForFlowReleasePO() {
        return waitingForFlowReleasePO;
    }

    public void setWaitingForFlowReleasePO(POSendMessage waitingForFlowReleasePO) {
        this.waitingForFlowReleasePO = waitingForFlowReleasePO;
    }

    protected DeliveryTag createDeliveryTag() {
        byte[] dtag = new byte[8];
        Util.writeLong(deliveryCountSnd, dtag, 0);
        return new DeliveryTag(dtag);
    }

    protected void recover(AMQPMap remoteUnsettled) {
        try {
            if (remoteUnsettled != null) {
                Map<AMQPType, AMQPType> map = remoteUnsettled.getValue();
                for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    final DeliveryTag deliveryTag = new DeliveryTag(((AMQPBinary) entry.getKey()).getValue());
                    final AMQPList deliveryState = (AMQPList) entry.getValue();
                    if (deliveryState != null) {
                        try {
                            DeliveryStateFactory.create(deliveryState).accept(new DeliveryStateVisitorAdapter() {
                                public void visit(Accepted impl) {
                                    deliveryMemory.deliverySettled(deliveryTag);
                                    mySession.dispatch(new POSendResumedTransfer(Producer.this, deliveryTag));
                                }
                            });
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            if (deliveryMemory.getNumberUnsettled() > 0) {
                Collection<UnsettledDelivery> unsettled = deliveryMemory.getUnsettled();
                for (Iterator<UnsettledDelivery> iter = unsettled.iterator(); iter.hasNext(); ) {
                    UnsettledDelivery unsettledDelivery = iter.next();
                    if (unsettledDelivery.getMessage() != null) {
                        AMQPMessage msg = unsettledDelivery.getMessage();
                        if (msg.getTxnIdIF() == null) {
                            POSendMessage po = new POSendMessage(null, this, msg, null, unsettledDelivery.getDeliveryTag());
                            po.setRecovery(true);
                            mySession.dispatch(po);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized String nextMsgId() {
        StringBuffer b = new StringBuffer(uniqueId);
        b.append('/');
        b.append(msgId++);
        return b.toString();
    }

    /**
     * Send a message to the target. For transactional sends the method returns after settlement has been finished, otherwise when the message has been sent.
     *
     * @param msg        message
     * @param persistent whether the message should send/stored durable
     * @param priority   message priority (default is 5)
     * @param ttl        time to live (expiration) in milliseconds, default no expiration
     * @return delivery state of the message
     * @throws AMQPException on error
     */
    public DeliveryStateIF send(AMQPMessage msg, boolean persistent, int priority, long ttl) throws AMQPException {
        verifyState();
        Header header = msg.getHeader();
        if (header == null) {
            header = new Header();
            msg.setHeader(header);
        }
        header.setDurable(new AMQPBoolean(persistent));
        header.setPriority(new AMQPUnsignedByte(priority));
        if (ttl >= 0)
            header.setTtl(new Milliseconds(ttl));

        Properties props = msg.getProperties();
        if (props == null) {
            props = new Properties();
            msg.setProperties(props);
        }
        if (props.getMessageId() == null)
            props.setMessageId(new MessageIdString(nextMsgId()));
        props.setTo(new AddressString(target));
        String userName = mySession.myConnection.getUserName();
        if (userName != null)
            props.setUserId(new AMQPBinary(userName.getBytes()));

        Semaphore sem = new Semaphore();
        try {
            POSendMessage po = new POSendMessage(sem, this, msg, msg.getTxnIdIF(), msg.getDeliveryTag());
            mySession.dispatch(po);
            sem.waitHere();
            if (!po.isSuccess())
                throw new AMQPException(po.getException());
            return po.getDeliveryState();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AMQPException(e.toString());
        }
    }

    /**
     * Send a message to the target with default persistence, default priority, default time to live.
     * For transactional sends the method returns after settlement has been finished, otherwise when the message has been sent.
     *
     * @param msg
     * @return delivery state of the message
     * @throws AMQPException
     */
    public DeliveryStateIF send(AMQPMessage msg) throws AMQPException {
        return send(msg, defaultPersistent, defaultPriority, defaultTtl);
    }

    protected void cancel() {
        if (waitingForFlowReleasePO != null && waitingForFlowReleasePO.getSemaphore() != null) {
            waitingForFlowReleasePO.setSuccess(false);
            waitingForFlowReleasePO.setException("Link has been cancelled");
            waitingForFlowReleasePO.getSemaphore().notifySingleWaiter();
            waitingForFlowReleasePO = null;
        }
        super.cancel();
    }
}
