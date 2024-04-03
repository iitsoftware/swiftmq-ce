/*
 * Copyright 2024 IIT Software GmbH
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

package com.swiftmq.impl.queue.standard.queue;

import com.swiftmq.tools.collection.ConcurrentExpandableList;
import com.swiftmq.tools.collection.ExpandableList;

import java.util.ArrayList;
import java.util.List;

public class ActiveTransactionRegistry {
    private ExpandableList<List<StoreId>> activeTransactions = new ConcurrentExpandableList<>();

    public int ensureTransaction(TransactionId transactionId) {
        int txId = transactionId.getTxId();
        if (txId == -1) {
            List<StoreId> transactionList = new ArrayList<>();
            txId = activeTransactions.add(transactionList);
            transactionId.setTxId(txId);
            transactionId.setTxList(transactionList);
        }
        return txId;
    }

    public List<StoreId> getTransactionList(int transactionId) {
        return activeTransactions.get(transactionId);
    }

    public void removeTransaction(int transactionId) {
        activeTransactions.remove(transactionId);
    }
}
