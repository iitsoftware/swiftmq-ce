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

package com.swiftmq.swiftlet.xa;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.QueueTransaction;

/**
 * This class acts as a context for an XA transaction. Multiple so-called 'threads of
 * control' (ToC) can share an XAContext. Before they access it, they have to register itself to
 * become associated with the XA transaction. Thereafter they do their work and add
 * QueueTransactions to this context before they unregister. If no more ToCs are registered
 * with the XAContext, the 2-phase-commit protocol is used to finish the transaction.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public interface XAContext {
    /**
     * Returns the Xid.
     *
     * @return xid.
     */
    public XidImpl getXid();

    /**
     * Sets this context to the 'prepared' state
     *
     * @param b true/false
     */
    public void setPrepared(boolean b);

    /**
     * Returns whether this transaction is in the 'prepared' state.
     *
     * @return true/false
     */
    public boolean isPrepared();

    /**
     * Returns whether this transaction was recovered on startup from the prepared log
     *
     * @return true/false
     */
    public boolean isRecovered();

    /**
     * Registers a new Thread of Control at this XAContext.
     *
     * @param description A description of the ToC
     * @return id ToC id
     * @throws XAContextException on error
     */
    public int register(String description) throws XAContextException;

    /**
     * Add a QueueTransaction to the XA transaction. This has to be done while the ToC is registered.
     *
     * @param id          the ToC id
     * @param queueName   Name of the queue
     * @param transaction QueueTransaction
     * @throws XAContextException on error
     */
    public void addTransaction(int id, String queueName, QueueTransaction transaction) throws XAContextException;

    /**
     * Unregisters a ToC from this XAContext.
     *
     * @param id           ToC id
     * @param rollbackOnly marks this transaction as rollback only
     * @throws XAContextException on error
     */
    public void unregister(int id, boolean rollbackOnly) throws XAContextException;

    /**
     * Performs a prepare operation on all registered QueueTransactions. All ToCs have to be unregistered to this.
     *
     * @throws XAContextException on error
     */
    public void prepare() throws XAContextException;

    /**
     * Performs a commit operation on all registered QueueTransactions. If 'onePhase' is set to true, a normal
     * commit is performed, otherwise a 2PC commit is performed.  All ToCs have to be unregistered to this. For 2PC
     * commit the XAContext has to be in the prepared state (isPrepared() must return true).
     *
     * @param onePhase perform a 1PC commit.
     * @return Flowcontrol delay
     * @throws XAContextException on error.
     */
    public long commit(boolean onePhase) throws XAContextException;

    /**
     * Performs a rollback operation on all registered QueueTransaction. If the XAContext is in the prepared state
     * (isPrepared() return true), a 2PC rollback is performed, otherwise a 1PC rollback (normal rollback) is performed.
     * All ToCs have to be unregistered to this.
     *
     * @throws XAContextException
     */
    public void rollback() throws XAContextException;

    /**
     * Closes this XAContext. This method is called when the XAResourceManagerSwiftlet shuts down. If the XAContext is
     * not in the prepared state, a normal rollback is performed on all registered QueueTransactions.
     */
    public void close();
}
