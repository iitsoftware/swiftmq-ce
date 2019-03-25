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

import com.swiftmq.swiftlet.Swiftlet;

import java.util.List;

/**
 * The StoreSwiftlet manages persistent, non-persistent, durable subscriber, and
 * XA stores.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class StoreSwiftlet extends Swiftlet {

    /**
     * Returns the persistent store for a queue.
     *
     * @param queueName queue name.
     * @return persistent store.
     * @throws StoreException on error.
     */
    public abstract PersistentStore getPersistentStore(String queueName)
            throws StoreException;

    /**
     * Returns the non-persistent store for a queue.
     *
     * @param queueName queue name.
     * @return persistent store.
     * @throws StoreException on error.
     */
    public abstract NonPersistentStore getNonPersistentStore(String queueName)
            throws StoreException;

    /**
     * Returns the durable subscriber store.
     *
     * @return durable subscriber store.
     * @throws StoreException on error.
     */
    public abstract DurableSubscriberStore getDurableSubscriberStore()
            throws StoreException;

    /**
     * Returns a list of all prepared log records.
     *
     * @return list of log records or null.
     * @throws StoreException on error.
     */
    public abstract List getPrepareLogRecords()
            throws StoreException;

    /**
     * Removes a prepared log record.
     *
     * @param record prepared log record.
     * @throws StoreException on error.
     */
    public abstract void removePrepareLogRecord(PrepareLogRecord record)
            throws StoreException;

    /**
     * Creates a new composite store transaction
     *
     * @return new composite store transaction
     * @throws StoreException on error.
     */
    public abstract CompositeStoreTransaction createCompositeStoreTransaction();
}

