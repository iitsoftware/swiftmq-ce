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

package com.swiftmq.impl.streams.comp.memory;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.impl.streams.TransactionFinishListener;
import com.swiftmq.impl.streams.TransactionFlushListener;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TxBookKeeper implements TransactionFlushListener {
    final static int OP_ADD = 0;
    final static int OP_REMOVE = 1;

    StreamContext ctx;
    String queueName;

    List<Entry> txLog = new ArrayList<Entry>();
    QueueSender sender = null;
    QueueReceiver receiver = null;

    public TxBookKeeper(StreamContext ctx, String queueName) throws Exception {
        this.ctx = ctx;
        this.queueName = queueName;
        sender = ctx.ctx.queueManager.createQueueSender(queueName, null);
        receiver = ctx.ctx.queueManager.createQueueReceiver(queueName, null, null);
        ctx.addTransactionFlushListener(this);
    }

    void add(String key, Message message, QueueMemory.KeyEntry keyEntry) {
        txLog.add(new Entry(key, OP_ADD, message, keyEntry));
    }

    void remove(String key, QueueMemory.KeyEntry keyEntry) {
        for (Iterator<Entry> iter = txLog.iterator(); iter.hasNext(); ) {
            Entry entry = iter.next();
            if (entry.op == OP_ADD && entry.key.equals(key)) {
                iter.remove();
                return;
            }
        }
        txLog.add(new Entry(key, OP_REMOVE, null, keyEntry));
    }

    Message get(String key) {
        for (ListIterator<Entry> iter = txLog.listIterator(txLog.size()); iter.hasPrevious(); ) {
            Entry entry = iter.previous();
            if (entry.op == OP_ADD && entry.key.equals(key))
                return entry.message;
        }
        return null;
    }

    boolean isRemoved(String key) {
        for (ListIterator<Entry> iter = txLog.listIterator(txLog.size()); iter.hasPrevious(); ) {
            Entry entry = iter.previous();
            if (entry.op == OP_REMOVE && entry.key.equals(key))
                return true;
        }
        return false;
    }

    void getSelected(MessageSelector selector, Memory result) throws Exception {
        for (Iterator<Entry> iter = txLog.iterator(); iter.hasNext(); ) {
            Entry entry = iter.next();
            if (entry.op == OP_ADD && entry.message.isSelected(selector)) {
                result.add(entry.message);
            }
        }
    }

    void remove(MessageSelector selector) throws Exception {
        for (Iterator<Entry> iter = txLog.iterator(); iter.hasNext(); ) {
            Entry entry = iter.next();
            if (entry.op == OP_ADD && entry.message.isSelected(selector)) {
                iter.remove();
            }
        }
    }

    @Override
    public void flush() {
        List<MessageIndex> toRemove = null;
        for (Iterator<Entry> iter = txLog.iterator(); iter.hasNext(); ) {
            Entry entry = iter.next();
            if (entry.op == OP_ADD) {
                try {
                    QueuePushTransaction transaction = sender.createTransaction();
                    transaction.putMessage(entry.message.getImpl());
                    ctx.addTransaction(transaction, new TxFinisher(entry.message.getImpl(), entry.keyEntry));
                } catch (QueueException e) {
                    e.printStackTrace();
                }
            } else {
                if (toRemove == null)
                    toRemove = new ArrayList<MessageIndex>();
                toRemove.add(entry.keyEntry.messageIndex);
            }
        }
        txLog.clear();
        if (toRemove != null) {
            try {
                QueuePullTransaction transaction = receiver.createTransaction(false);
                transaction.removeMessages(toRemove);
                ctx.addTransaction(transaction, null);
            } catch (QueueException e) {
                e.printStackTrace();
            }
        }
    }

    void close() {
        ctx.removeTransactionFlushListener(this);
        try {
            sender.close();
        } catch (QueueException e) {
        }
        try {
            receiver.close();
        } catch (QueueException e) {
        }

    }

    @Override
    public String toString() {
        return "TxBookKeeper{" +
                "queueName='" + queueName + '\'' +
                '}';
    }

    private class Entry {
        String key;
        int op;
        Message message;
        QueueMemory.KeyEntry keyEntry;

        public Entry(String key, int op, Message message, QueueMemory.KeyEntry keyEntry) {
            this.key = key;
            this.op = op;
            this.message = message;
            this.keyEntry = keyEntry;
        }

        public Entry(String key, int op) {
            this.key = key;
            this.op = op;
        }
    }

    private class TxFinisher implements TransactionFinishListener {
        MessageImpl impl;
        QueueMemory.KeyEntry keyEntry;

        public TxFinisher(MessageImpl impl, QueueMemory.KeyEntry keyEntry) {
            this.impl = impl;
            this.keyEntry = keyEntry;
        }

        @Override
        public void transactionFinished() {
            if (impl != null) {
                // To keep the data sync
                synchronized (this) {
                    keyEntry.messageIndex = (MessageIndex) impl.getStreamPKey();
                }
            }
        }
    }
}
