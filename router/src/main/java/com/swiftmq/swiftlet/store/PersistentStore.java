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

import java.util.List;

/**
 * A store for persistent messages.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface PersistentStore {

    /**
     * Returns a list of all store entries
     *
     * @return list of all store entries.
     * @throws StoreException on error.
     */
    public List getStoreEntries()
            throws StoreException;

    /**
     * Get a store entry.
     *
     * @param key the key.
     * @return store entry.
     * @throws StoreException on error.
     */
    public StoreEntry get(Object key)
            throws StoreException;

    /**
     * Delete the persistent store incl. all entries.
     *
     * @throws StoreException on error.
     */
    public void delete()
            throws StoreException;

    /**
     * Create a new read transaction.
     *
     * @param markRedelivered states whether messages should be marked as redelivered on rollback.
     * @return new transaction.
     * @throws StoreException on error.
     */
    public StoreReadTransaction createReadTransaction(boolean markRedelivered)
            throws StoreException;

    /**
     * Create a new write transaction.
     *
     * @return new transaction.
     * @throws StoreException on error.
     */
    public StoreWriteTransaction createWriteTransaction()
            throws StoreException;

    /**
     * Close the store.
     *
     * @throws StoreException on error.
     */
    public void close()
            throws StoreException;
}

