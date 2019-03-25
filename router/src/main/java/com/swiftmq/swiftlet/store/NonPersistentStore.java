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

/**
 * A store for non-persistent messages (a swap store).
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface NonPersistentStore {

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
     * Update the delivery count of a store entry.
     *
     * @param key           the key.
     * @param deliveryCount new count.
     * @throws StoreException on error.
     */
    public void updateDeliveryCount(Object key, int deliveryCount)
            throws StoreException;

    /**
     * Insert a store entry.
     *
     * @param storeEntry store entry.
     * @throws StoreException on error.
     */
    public void insert(StoreEntry storeEntry)
            throws StoreException;

    /**
     * Delete a store entry.
     *
     * @param key the key.
     * @throws StoreException on error.
     */
    public void delete(Object key)
            throws StoreException;


    /**
     * Close the store and delete all content.
     *
     * @throws StoreException on error.
     */
    public void close()
            throws StoreException;
}

