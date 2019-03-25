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

import java.util.Iterator;

/**
 * The DurableSubscriberStore.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface DurableSubscriberStore {

    /**
     * Returns an iterator for all DurableStoreEntries.
     *
     * @return iterator.
     * @throws StoreException on error.
     */
    public Iterator iterator()
            throws StoreException;


    /**
     * Returns a DurableStoreEntry.
     *
     * @param clientId    client id.
     * @param durableName durable name.
     * @return entry or null.
     * @throws StoreException on error.
     */
    public DurableStoreEntry getDurableStoreEntry(String clientId, String durableName)
            throws StoreException;


    /**
     * Insert a new DurableStoreEntry.
     *
     * @param durableStoreEntry entry.
     * @throws StoreException on error.
     */
    public void insertDurableStoreEntry(DurableStoreEntry durableStoreEntry)
            throws StoreException;


    /**
     * Deletes a DurableStoreEntry.
     *
     * @param clientId    client id.
     * @param durableName durable name.
     * @throws StoreException on error.
     */
    public void deleteDurableStoreEntry(String clientId, String durableName)
            throws StoreException;


    /**
     * Closes the store.
     *
     * @throws StoreException on error.
     */
    public void close()
            throws StoreException;
}

