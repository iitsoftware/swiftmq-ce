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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.store.PrepareLogRecord;
import com.swiftmq.swiftlet.store.StoreTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TransactionId covers type of transaction and internal id
 */
public class TransactionId {
    public final static int PULL_TRANSACTION = 0;
    public final static int PUSH_TRANSACTION = 1;
    int transactionType = PULL_TRANSACTION;
    boolean prepared = false;
    boolean alreadyStored = false;
    StoreTransaction storeTransaction = null;
    XidImpl globalTxId = null;
    List preparedList = null;
    PrepareLogRecord logRecord = null;
    List txList = null;
    int txId = -1;

    public TransactionId(int transactionType) {
        this.transactionType = transactionType;
        this.txList = Collections.synchronizedList(new ArrayList());
    }

    public TransactionId() {
    }

    public int getTransactionType() {
        return (transactionType);
    }

    public int getTxId() {
        return txId;
    }

    public void setTxId(int txId) {
        this.txId = txId;
    }

    public void setTxList(List txList) {
        this.txList = txList;
    }

    public List getTxList() {
        return txList;
    }

    void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    boolean isPrepared() {
        return prepared;
    }

    boolean isAlreadyStored() {
        return alreadyStored;
    }

    void setAlreadyStored(boolean alreadyStored) {
        this.alreadyStored = alreadyStored;
    }

    PrepareLogRecord getLogRecord() {
        return logRecord;
    }

    void setLogRecord(PrepareLogRecord logRecord) {
        this.logRecord = logRecord;
    }

    StoreTransaction getStoreTransaction() {
        return storeTransaction;
    }

    void setStoreTransaction(StoreTransaction storeTransaction) {
        this.storeTransaction = storeTransaction;
    }

    XidImpl getGlobalTxId() {
        return globalTxId;
    }

    void setGlobalTxId(XidImpl globalTxId) {
        this.globalTxId = globalTxId;
    }

    List getPreparedList() {
        return preparedList;
    }

    void setPreparedList(List preparedList) {
        this.preparedList = preparedList;
    }

    void clear() {
        prepared = false;
        storeTransaction = null;
        globalTxId = null;
        preparedList = null;
        logRecord = null;
        txList = null;
        txId = -1;
    }

    public String toString() {
        return (transactionType == PULL_TRANSACTION ? "PULL_TRANSACTION" : "PUSH_TRANSACTION") + "TxId=" + txId;
    }
}

