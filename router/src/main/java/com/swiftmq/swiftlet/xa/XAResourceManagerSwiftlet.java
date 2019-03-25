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
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.queue.QueueTransaction;

import java.util.List;

/**
 * The XAResourceManagerSwiftlet manages XA in-doubt transactions which are
 * prepared but uncommitted XA transactions. The XAResourceManagerSwiftlet
 * acts as a global XA resource of a SwiftMQ router.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2003, All Rights Reserved
 */
public abstract class XAResourceManagerSwiftlet extends Swiftlet {
    /**
     * Set the transaction timeout. Overwrites the default tx timeout.
     *
     * @param timeout
     */
    public abstract void setTransactionTimeout(long timeout);

    /**
     * Returns the transaction timeout. If not set, the default tx timeout is returned.
     *
     * @return timeout
     */

    public abstract long getTransactionTimeout();

    /**
     * Returns whether the Xid has been completed heuristically.
     *
     * @param xid xid.
     * @return true/false.
     */
    public abstract boolean isHeuristicCompleted(XidImpl xid);

    /**
     * Returns whether the Xid has been committed heuristically.
     *
     * @param xid xid.
     * @return true/false.
     */
    public abstract boolean isHeuristicCommit(XidImpl xid);

    /**
     * Returns whether the Xid has been rolled back heuristically.
     *
     * @param xid xid.
     * @return true/false.
     */
    public abstract boolean isHeuristicRollback(XidImpl xid);

    /**
     * Returns a list of heuristic completed Xids.
     *
     * @return list of heuristically completed Xids.
     */
    public abstract List getHeuristicCompletedXids();

    /**
     * Forgets a heuristic completed XA transaction.
     *
     * @param xid Xid.
     */
    public abstract void forget(XidImpl xid);

    /**
     * Returns whether the Xid is stored.
     *
     * @param xid xid.
     * @return true/false.
     */
    public abstract boolean hasPreparedXid(XidImpl xid);

    /**
     * Returns a list of prepared Xids.
     *
     * @return list of prepared Xids.
     */
    public abstract List getPreparedXids();

    /**
     * Returns a list of prepared Xids that matches the XidFilter
     *
     * @param filter the XidFilter
     * @return list of prepared Xids.
     */
    public abstract List getPreparedXids(XidFilter filter);

    /**
     * Creates a new XAContext for the given Xid. If an XAContext is
     * already defined for the Xid, the method returns null.
     *
     * @param xid xid.
     * @return XAContext or null if already defined.
     */
    public abstract XAContext createXAContext(XidImpl xid);

    /**
     * Returns an existing XAContext for the given Xid. If the XAContext
     * is not defined, the method return null.
     *
     * @param xid xid.
     * @return XAContext or null if not defined.
     */
    public abstract XAContext getXAContext(XidImpl xid);

    /**
     * Removes an existing XAContext for the given Xid. The XAContext is closed.
     *
     * @param xid xid.
     */
    public abstract void removeXAContext(XidImpl xid);

    /* The following methods are for backward compatibility to SwiftMQ 4.x */

    /**
     * Passes a prepared XA transaction into the responsibility of the XAResourceManagerSwiftlet.
     *
     * @param xid         Xid.
     * @param queueName   queue name.
     * @param transaction open queue transaction.
     * @deprecated Use XAContext in future
     */
    public abstract void addPreparedTransaction(XidImpl xid, String queueName, QueueTransaction transaction);

    /**
     * Commits a prepared XA transaction.
     *
     * @param xid Xid.
     * @deprecated Use XAContext in future
     */
    public abstract void commit(XidImpl xid);

    /**
     * Aborts a prepared XA transaction.
     *
     * @param xid Xid.
     * @deprecated Use XAContext in future
     */
    public abstract void rollback(XidImpl xid);

}
