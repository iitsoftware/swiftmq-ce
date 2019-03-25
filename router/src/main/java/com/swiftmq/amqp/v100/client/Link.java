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

import com.swiftmq.amqp.v100.client.po.POCloseLink;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.DeliveryTag;
import com.swiftmq.amqp.v100.generated.transport.definitions.Error;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;

import java.util.Set;

/**
 * <p>
 * Base class for links, created from a session.
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class Link {
    Session mySession;
    String name;
    int qoS = 0;
    volatile Set<AMQPSymbol> offeredCapabilities = null;
    volatile Set<AMQPSymbol> desiredCapabilities = null;
    volatile Set<AMQPSymbol> destinationCapabilities = null;
    volatile long maxMessageSize = 0;
    volatile int handle;
    volatile long remoteHandle;
    volatile AddressIF remoteAddress;
    volatile POObject waitingPO = null;
    volatile POObject waitingClosePO = null;
    volatile boolean closed = false;
    DeliveryMemory deliveryMemory = null;
    volatile com.swiftmq.amqp.v100.generated.transport.definitions.Error error = null;

    protected Link(Session mySession, String name, int qoS, DeliveryMemory deliveryMemory) {
        this.mySession = mySession;
        this.name = name;
        this.qoS = qoS;
        this.deliveryMemory = deliveryMemory;
    }

    protected void verifyState() throws LinkClosedException {
        if (closed)
            throw new LinkClosedException("Link is closed" + (error != null ? ": " + error.getCondition().getValueString() + "/" + error.getDescription().getValue() : ""));
    }

    /**
     * Returns the session.
     *
     * @return session
     */
    public Session getMySession() {
        return mySession;
    }

    /**
     * Returns the name of this link.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the quality of service of this link.
     *
     * @return quality of service
     */
    public int getQoS() {
        return qoS;
    }

    protected void setHandle(int handle) {
        this.handle = handle;
    }

    /**
     * Return the handle of this link.
     *
     * @return handle
     */
    public int getHandle() {
        return handle;
    }

    protected void setRemoteHandle(long remoteHandle) {
        this.remoteHandle = remoteHandle;
    }

    /**
     * Returns the remote address of this link.
     *
     * @return remote address
     */
    public AddressIF getRemoteAddress() {
        return remoteAddress;
    }

    protected void setRemoteAddress(AddressIF remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    /**
     * Return the delivery memory used by this link
     *
     * @return delivery memory
     */
    public DeliveryMemory getDeliveryMemory() {
        return deliveryMemory;
    }

    protected void setDeliveryCount(long deliveryCount) {
    }

    protected long getRemoteHandle() {
        return remoteHandle;
    }

    /**
     * Returns the offered capabilities.
     *
     * @return offered capabilities
     */
    public Set<AMQPSymbol> getOfferedCapabilities() {
        return offeredCapabilities;
    }

    /**
     * Sets the offered capabilities.
     *
     * @param offeredCapabilities offered capabilities
     */
    public void setOfferedCapabilities(Set<AMQPSymbol> offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
    }

    /**
     * Returns the desired capabilities.
     *
     * @return desired capabilities
     */
    public Set<AMQPSymbol> getDesiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * Sets the desired capabilities.
     *
     * @param desiredCapabilities desired capabilities
     */
    public void setDesiredCapabilities(Set<AMQPSymbol> desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
    }

    /**
     * Returns the destination capabilities
     *
     * @return destination capabilities
     */
    public Set<AMQPSymbol> getDestinationCapabilities() {
        return destinationCapabilities;
    }

    protected void setDestinationCapabilities(Set<AMQPSymbol> destinationCapabilities) {
        this.destinationCapabilities = destinationCapabilities;
    }

    /**
     * Returns the maximum message size in bytes
     *
     * @return max message size
     */
    public long getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Sets the maximum message size in bytes. Default is unlimited.
     *
     * @param maxMessageSize max message size
     */
    public void setMaxMessageSize(long maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    protected POObject getWaitingPO() {
        return waitingPO;
    }

    protected void setWaitingPO(POObject waitingPO) {
        this.waitingPO = waitingPO;
    }

    protected POObject getWaitingClosePO() {
        return waitingClosePO;
    }

    protected void setWaitingClosePO(POObject waitingClosePO) {
        this.waitingClosePO = waitingClosePO;
    }

    protected DeliveryTag createDeliveryTag() {
        return null;
    }

    protected void cancel() {
        closed = true;
        if (waitingPO != null && waitingPO.getSemaphore() != null) {
            waitingPO.setSuccess(false);
            waitingPO.setException("Link has been cancelled");
            waitingPO.getSemaphore().notifySingleWaiter();
            waitingPO = null;
        }
        if (waitingClosePO != null && waitingClosePO.getSemaphore() != null) {
            waitingClosePO.setSuccess(false);
            waitingClosePO.setException("Link has been cancelled");
            waitingClosePO.getSemaphore().notifySingleWaiter();
            waitingClosePO = null;
        }
    }

    protected void remoteDetach(Error error) {
        this.error = error;
        mySession.detach(this);
        cancel();
        POCloseLink po = new POCloseLink(null, this);
        mySession.getSessionDispatcher().dispatch(po);
    }

    /**
     * Indicates whether this link is closed.
     *
     * @return closed true/false
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Close this link.
     *
     * @throws AMQPException on error
     */
    public void close() throws AMQPException {
        if (closed)
            return;
        Semaphore sem = new Semaphore();
        POCloseLink po = new POCloseLink(sem, this);
        mySession.getSessionDispatcher().dispatch(po);
        sem.waitHere();
        mySession.detach(this);
        closed = true;
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
    }
}
