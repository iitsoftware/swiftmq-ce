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

package com.swiftmq.swiftlet.store;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;

/**
 * Base class for store transactions.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2004, All Rights Reserved
 */
public interface StoreTransaction {
    /**
     * Prepares a 2PC transaction.
     *
     * @param globalTxId global tx id.
     * @throws StoreException on error.
     */
    public void prepare(XidImpl globalTxId)
            throws StoreException;

    /**
     * Commits a 2PC transaction.
     *
     * @param globalTxId global tx id.
     * @throws StoreException on error.
     */
    public void commit(XidImpl globalTxId)
            throws StoreException;

    /**
     * Commits a local transaction.
     *
     * @throws StoreException on error.
     */
    public void commit()
            throws StoreException;

    /**
     * Asynchronously commits a local transaction.
     *
     * @param callback async completion callback (may be null)
     */
    public void commit(AsyncCompletionCallback callback);

    /**
     * Aborts a 2PC transaction.
     *
     * @param globalTxId global tx id.
     * @throws StoreException on error.
     */
    public void abort(XidImpl globalTxId)
            throws StoreException;

    /**
     * Aborts a local transaction.
     *
     * @throws StoreException on error.
     */
    public void abort()
            throws StoreException;

    /**
     * Asynchronously aborts a local transaction.
     *
     * @param callback async completion callback (may be null)
     */
    public void abort(AsyncCompletionCallback callback);
}

