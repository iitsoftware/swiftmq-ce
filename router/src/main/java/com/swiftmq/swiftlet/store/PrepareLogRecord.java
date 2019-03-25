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

import java.util.List;

/**
 * A prepare log record, used to prepare 2PC XA transactions.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface PrepareLogRecord {
    public static final int READ_TRANSACTION = 0;
    public static final int WRITE_TRANSACTION = 1;

    /**
     * Returns the transaction type.
     *
     * @return transaction type.
     */
    public int getType();

    /**
     * Returns the global tx id.
     *
     * @return global tx id.
     */
    public XidImpl getGlobalTxId();

    /**
     * Returns the queue name.
     *
     * @return queue name.
     */
    public String getQueueName();

    /**
     * Returns a list of keys in the persistent store this transaction refers to.
     *
     * @return key list.
     */
    public List getKeyList();
}
