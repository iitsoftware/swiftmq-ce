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
import com.swiftmq.amqp.v100.client.po.*;
import com.swiftmq.amqp.v100.generated.messaging.addressing.TerminusExpiryPolicy;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Session, created from a Connection.
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class Session {
    AMQPContext ctx = null;
    Connection myConnection = null;
    int channel = 0;
    int remoteChannel = 0;
    long incomingWindowSize = 1;
    long outgoingWindowSize = 1;
    SessionDispatcher sessionDispatcher = null;
    TransactionController transactionController = null;
    Set links = new HashSet();
    Lock lock = new ReentrantLock();
    boolean closed = false;
    Error error = null;

    protected Session(AMQPContext ctx, Connection myConnection, long incomingWindowSize, long outgoingWindowSize) {
        this.ctx = ctx;
        this.myConnection = myConnection;
        this.incomingWindowSize = incomingWindowSize;
        this.outgoingWindowSize = outgoingWindowSize;
        sessionDispatcher = new SessionDispatcher(ctx, this, myConnection.getOutboundHandler());
    }

    protected void finishHandshake() throws SessionHandshakeException {
        Semaphore sem = new Semaphore();
        POObject po = new POBegin(sem);
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        sem.reset();
        if (!po.isSuccess()) {
            cancel();
            throw new SessionHandshakeException(po.getException());
        }
    }

    private void verifyState() throws SessionClosedException {
        if (closed)
            throw new SessionClosedException("Session is closed" + (error != null ? ": " + error.getCondition().getValueString() + "/" + error.getDescription().getValue() : ""));
    }

    protected void setChannel(int channel) {
        this.channel = channel;
        sessionDispatcher.setMyChannel(channel);
    }

    /**
     * Returns the channel.
     *
     * @return channel
     */
    public int getChannel() {
        return channel;
    }

    protected SessionDispatcher getSessionDispatcher() {
        return sessionDispatcher;
    }

    protected int getRemoteChannel() {
        return remoteChannel;
    }

    protected void setRemoteChannel(int remoteChannel) {
        this.remoteChannel = remoteChannel;
    }

    /**
     * Return the incoming window size.
     *
     * @return incoming window size
     */
    public long getIncomingWindowSize() {
        return incomingWindowSize;
    }

    /**
     * Return the outgoing window size.
     *
     * @return outgoing window size
     */
    public long getOutgoingWindowSize() {
        return outgoingWindowSize;
    }

    /**
     * Creates a message producer on a target.
     *
     * @param target the target, e.g. queue name
     * @param qoS    the quality of service
     * @return message producer
     * @throws AMQPException on error
     */
    public Producer createProducer(String target, int qoS) throws AMQPException {
        return createProducer(target, qoS, null);
    }

    /**
     * Creates a message producer on a target.
     *
     * @param target         the target, e.g. queue name
     * @param qoS            the quality of service
     * @param deliveryMemory delivery memory for recovery
     * @return message producer
     * @throws AMQPException on error
     */
    public Producer createProducer(String target, int qoS, DeliveryMemory deliveryMemory) throws AMQPException {
        verifyState();
        QoS.verify(qoS);
        Semaphore sem = new Semaphore();
        POAttachProducer po = new POAttachProducer(sem, target, qoS, deliveryMemory == null ? new DefaultDeliveryMemory() : deliveryMemory);
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        Producer p = (Producer) po.getLink();
        links.add(p);
        return p;
    }

    /**
     * Creates a message consumer on a source.
     *
     * @param source     the source, e.g. queue name
     * @param linkCredit link credit
     * @param qoS        quality of service
     * @param noLocal    if true means it won't receive messages sent on the same topic and connection
     * @param selector   message selector (for SwiftMQ this would be a JMS message selector string)
     * @return message consumer
     * @throws AMQPException on error
     */
    public Consumer createConsumer(String source, int linkCredit, int qoS, boolean noLocal, String selector) throws AMQPException {
        return createConsumer(source, linkCredit, qoS, noLocal, selector, null);
    }

    /**
     * Creates a message consumer on a source.
     *
     * @param source         the source, e.g. queue name
     * @param linkCredit     link credit
     * @param qoS            quality of service
     * @param noLocal        if true means it won't receive messages sent on the same topic and connection
     * @param selector       message selector (for SwiftMQ this would be a JMS message selector string)
     * @param deliveryMemory delivery memory for recovery
     * @return message consumer
     * @throws AMQPException on error
     */
    public Consumer createConsumer(String source, int linkCredit, int qoS, boolean noLocal, String selector, DeliveryMemory deliveryMemory) throws AMQPException {
        verifyState();
        QoS.verify(qoS);
        Semaphore sem = new Semaphore();
        POAttachConsumer po = new POAttachConsumer(sem, source, linkCredit, qoS, noLocal, selector, deliveryMemory == null ? new DefaultDeliveryMemory() : deliveryMemory);
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        Consumer c = (Consumer) po.getLink();
        links.add(c);
        return c;
    }

    /**
     * Creates a message consumer on a source without a link credit. This is required for transactional acquisition.
     *
     * @param source   the source, e.g. queue name
     * @param qoS      quality of service
     * @param noLocal  if true means it won't receive messages sent on the same topic and connection
     * @param selector message selector (for SwiftMQ this would be a JMS message selector string)
     * @return message consumer
     * @throws AMQPException on error
     */
    public Consumer createConsumer(String source, int qoS, boolean noLocal, String selector) throws AMQPException {
        verifyState();
        QoS.verify(qoS);
        Semaphore sem = new Semaphore();
        POAttachConsumer po = new POAttachConsumer(sem, source, -1, qoS, noLocal, selector, new DefaultDeliveryMemory());
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        Consumer c = (Consumer) po.getLink();
        links.add(c);
        return c;
    }

    /**
     * Creates a temporary destination and a message consumer on it. The temporary destination has a lifetime of
     * the connection.
     *
     * @param qoS quality of service
     * @return message consumer
     * @throws AMQPException on error
     */
    public Consumer createConsumer(int linkCredit, int qoS) throws AMQPException {
        verifyState();
        QoS.verify(qoS);
        Semaphore sem = new Semaphore();
        POAttachConsumer po = new POAttachConsumer(sem, null, linkCredit, qoS, false, null, new DefaultDeliveryMemory());
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        Consumer c = (Consumer) po.getLink();
        links.add(c);
        return c;
    }

    /**
     * Creates a durable message consumer on a topic. This is the same as a durable subscriber in JMS. The container id
     * and linkName is used to identify the durable consumer. A durable consumer is backed by a durable link which will
     * only be destroyed by calling "unsubscribe()" from the durable consumer.
     *
     * @param linkName   the name of the link
     * @param source     the source, e.g. queue name
     * @param linkCredit link credit
     * @param qoS        quality of service
     * @param noLocal    if true means it won't receive messages sent on the same topic and connection
     * @param selector   message selector (for SwiftMQ this would be a JMS message selector string)
     * @return message consumer
     * @throws AMQPException on error
     */
    public DurableConsumer createDurableConsumer(String linkName, String source, int linkCredit, int qoS, boolean noLocal, String selector) throws AMQPException {
        return createDurableConsumer(linkName, source, linkCredit, qoS, noLocal, selector, null);
    }

    /**
     * Creates a durable message consumer on a topic. This is the same as a durable subscriber in JMS. The container id
     * and linkName is used to identify the durable consumer. A durable consumer is backed by a durable link which will
     * only be destroyed by calling "unsubscribe()" from the durable consumer.
     *
     * @param linkName       the name of the link
     * @param source         the source, e.g. queue name
     * @param linkCredit     link credit
     * @param qoS            quality of service
     * @param noLocal        if true means it won't receive messages sent on the same topic and connection
     * @param selector       message selector (for SwiftMQ this would be a JMS message selector string)
     * @param deliveryMemory delivery memory for recovery
     * @return message consumer
     * @throws AMQPException on error
     */
    public DurableConsumer createDurableConsumer(String linkName, String source, int linkCredit, int qoS, boolean noLocal, String selector, DeliveryMemory deliveryMemory) throws AMQPException {
        verifyState();
        if (linkName == null)
            throw new AMQPException("Please specify the link name");
        if (!myConnection.containerIdSet)
            throw new AMQPException("Please specify a container id");
        QoS.verify(qoS);
        Semaphore sem = new Semaphore();
        POAttachDurableConsumer po = new POAttachDurableConsumer(sem, linkName, source, linkCredit, qoS, noLocal, selector, TerminusExpiryPolicy.NEVER, deliveryMemory == null ? new DefaultDeliveryMemory() : deliveryMemory);
        sessionDispatcher.dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        DurableConsumer d = (DurableConsumer) po.getLink();
        links.add(d);
        return d;
    }

    protected void detach(Link link) {
        lock.lock();
        try {
            links.remove(link);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the transaction controller of this session.
     *
     * @return transaction controller
     * @throws SessionClosedException if the session is closed
     */
    public synchronized TransactionController getTransactionController() throws SessionClosedException {
        verifyState();
        if (transactionController == null)
            transactionController = new TransactionController(this);
        return transactionController;
    }

    protected void dispatch(POObject po) {
        sessionDispatcher.dispatch(po);
    }

    protected void remoteEnd(com.swiftmq.amqp.v100.generated.transport.definitions.Error error) {
        this.error = error;
        cancel();
    }

    private Link[] getLinksCopy() {
        lock.lock();
        try {
            Link[] l = null;
            l = (Link[]) links.toArray(new Link[links.size()]);
            links.clear();
            return l;
        } finally {
            lock.unlock();
        }
    }

    protected void cancel() {
        if (closed)
            return;
        if (links.size() > 0) {
            Link[] l = getLinksCopy();
            for (int i = 0; i < l.length; i++)
                l[i].cancel();
        }
        myConnection.removeSession(this);
        myConnection.unmapSessionFromRemoteChannel(remoteChannel);
        sessionDispatcher.close();
        closed = true;
    }

    /**
     * Closes the session and all consumers/producers created from this session.
     */
    public void close() {
        lock.lock();
        if (closed)
            return;
        try {
            if (transactionController != null) {
                transactionController.close();
                transactionController = null;
            }
            Semaphore sem = new Semaphore();
            POSendEnd po = new POSendEnd(sem, null);
            sessionDispatcher.dispatch(po);
            sem.waitHere();
            cancel();
        } finally {
            lock.unlock();
        }
    }
}
