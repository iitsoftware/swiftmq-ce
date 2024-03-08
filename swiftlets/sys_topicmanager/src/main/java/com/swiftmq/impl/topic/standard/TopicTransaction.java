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

package com.swiftmq.impl.topic.standard;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.swiftlet.queue.QueueLimitException;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.CallbackJoin;

import java.util.ArrayList;
import java.util.List;

public class TopicTransaction {
    TopicManagerContext ctx = null;
    int transactionId = -1;
    TopicSubscriberTransaction[] subscriberTransactions = null;
    CompositeStoreTransaction parentTx = null;

    protected TopicTransaction(TopicManagerContext ctx) {
        this.ctx = ctx;
    }

    public void setParentTx(CompositeStoreTransaction parentTx) {
        this.parentTx = parentTx;
    }

    public void lockQueues() {
        if (subscriberTransactions != null) {
            for (TopicSubscriberTransaction sub : subscriberTransactions) {
                if (sub != null)
                    sub.getTransaction().lockQueue();
            }
        }
    }

    public void unlockQueues() {
        if (subscriberTransactions != null) {
            for (TopicSubscriberTransaction sub : subscriberTransactions) {
                if (sub != null)
                    sub.getTransaction().unlockQueue(false);   // it is never called async
            }
            subscriberTransactions = null;
        }
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    protected int getTransactionId() {
        return (transactionId);
    }

    protected void setTopicSubscriberTransaction(int brokerSubscriberId, TopicSubscriberTransaction topicSubscriberTransaction) {
        if (subscriberTransactions == null || subscriberTransactions.length - 1 < brokerSubscriberId) {
            TopicSubscriberTransaction[] ns = new TopicSubscriberTransaction[brokerSubscriberId + 1];
            if (subscriberTransactions != null)
                System.arraycopy(subscriberTransactions, 0, ns, 0, subscriberTransactions.length);
            subscriberTransactions = ns;
        }
        subscriberTransactions[brokerSubscriberId] = topicSubscriberTransaction;
    }

    protected TopicSubscriberTransaction getTopicSubscriberTransaction(int brokerSubscriberId) {
        if (subscriberTransactions == null || subscriberTransactions.length - 1 < brokerSubscriberId)
            return null;
        return subscriberTransactions[brokerSubscriberId];
    }

    protected void prepare(XidImpl globalTxId) throws Exception {
        if (subscriberTransactions == null)
            return;
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null) {
                try {
                    sub.prepare(globalTxId);
                } catch (QueueTransactionClosedException ignored) {
                }
            }
        }
    }

    protected long commit(XidImpl globalTxId) throws Exception {
        if (subscriberTransactions == null)
            return 0;
        long delay = 0;
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null) {
                try {
                    delay = Math.max(delay, sub.commit(globalTxId));
                } catch (QueueTransactionClosedException ignored) {
                }
            }
        }
        subscriberTransactions = null;
        return delay;
    }

    protected void rollback(XidImpl globalTxId) throws Exception {
        if (subscriberTransactions == null)
            return;
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null)
                try {
                    sub.rollback(globalTxId);
                } catch (QueueTransactionClosedException ignored) {
                }
        }
        subscriberTransactions = null;
    }

    protected long commit() throws Exception {
        if (subscriberTransactions == null)
            return 0;
        return parentTx != null ? parentTxCommit() : noParentTxCommit();
    }

    private long noParentTxCommit() throws Exception {
        long delay = 0;
        List<TopicSubscriberTransaction> durPersSubs = new ArrayList<>(subscriberTransactions.length);
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null) {
                TopicSubscription subscription = sub.getTopicSubscription();
                if (subscription.isRemote() || !subscription.isDurable() || !sub.isPersistentMessageIncluded()) {
                    try {
                        delay = Math.max(delay, sub.commit());
                    } catch (QueueTransactionClosedException ignored) {
                    }
                } else
                    durPersSubs.add(sub);
            }
        }
        int size = durPersSubs.size();
        if (size > 0) {
            if (size == 1) {
                TopicSubscriberTransaction sub = durPersSubs.get(0);
                try {
                    delay = Math.max(delay, sub.commit());
                } catch (QueueTransactionClosedException ignored) {
                }
            } else {
                int lastLocked = -1;
                boolean spaceLeft = true;
                for (int i = 0; i < size; i++) {
                    TopicSubscriberTransaction sub = durPersSubs.get(i);
                    sub.getTransaction().lockQueue();
                    lastLocked = i;
                    if (!sub.getTransaction().hasSpaceLeft()) {
                        spaceLeft = false;
                        break;
                    }
                }
                if (!spaceLeft) {
                    for (int i = 0; i <= lastLocked; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        sub.getTransaction().unlockQueue(false);
                    }
                    throw new QueueLimitException("Maximum Messages in at least one Durable Subscriber Queue reached!");
                }
                CompositeStoreTransaction compositeTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
                try {
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        try {
                            sub.getTransaction().setCompositeStoreTransaction(compositeTx);
                            delay = Math.max(delay, sub.commit());
                            sub.getTransaction().setCompositeStoreTransaction(null);
                        } catch (QueueTransactionClosedException ignored) {
                        }
                    }
                    compositeTx.commitTransaction();
                } finally {
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = (TopicSubscriberTransaction) durPersSubs.get(i);
                        sub.getTransaction().unlockQueue(false);
                    }
                }
            }
        }

        subscriberTransactions = null;
        return delay;
    }

    private long parentTxCommit() throws Exception {
        boolean wasReferencable = parentTx.isReferencable();
        parentTx.setReferencable(true);
        long delay = 0;
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null) {
                try {
                    sub.getTransaction().setCompositeStoreTransaction(parentTx);
                    delay = Math.max(delay, sub.commit());
                    sub.getTransaction().setCompositeStoreTransaction(null);
                } catch (QueueTransactionClosedException ignored) {
                }
            }
        }

        parentTx.setReferencable(wasReferencable);
        return delay;
    }

    protected void commit(AsyncCompletionCallback callback) {
        final DelayCollector delayCollector = new DelayCollector(callback);
        if (subscriberTransactions != null) {
            final List<TopicSubscriberTransaction> durPersSubs = new ArrayList<>(subscriberTransactions.length);
            for (TopicSubscriberTransaction sub : subscriberTransactions) {
                if (sub != null) {
                    TopicSubscription subscription = sub.getTopicSubscription();
                    if (subscription.isRemote() || !subscription.isDurable() || !sub.isPersistentMessageIncluded()) {
                        delayCollector.incNumberCallbacks();
                        sub.commit(new AsyncCompletionCallback() {
                            public void done(boolean success) {
                                delayCollector.done(this, success);
                            }
                        });
                    } else
                        durPersSubs.add(sub);
                }
            }
            final int size = durPersSubs.size();
            long delay = 0;
            if (size > 0) {
                // Only 1 subscriber, direct commit
                if (size == 1) {
                    TopicSubscriberTransaction sub = durPersSubs.get(0);
                    delayCollector.incNumberCallbacks();
                    sub.commit(new AsyncCompletionCallback() {
                        public void done(boolean success) {
                            delayCollector.done(this, success);
                        }
                    });
                } else {
                    // Multiple subscriber, composite tx
                    int lastLocked = -1;
                    boolean spaceLeft = true;
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        sub.getTransaction().lockQueue();
                        lastLocked = i;
                        if (!sub.getTransaction().hasSpaceLeft()) {
                            spaceLeft = false;
                            break;
                        }
                    }
                    if (!spaceLeft) {
                        for (int i = 0; i <= lastLocked; i++) {
                            TopicSubscriberTransaction sub = durPersSubs.get(i);
                            sub.getTransaction().unlockQueue(false);
                        }
                        callback.setException(new QueueLimitException("Maximum Messages in at least one Durable Subscriber Queue reached!"));
                        callback.notifyCallbackStack(false);
                        return;
                    }
                    CompositeStoreTransaction compositeTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        sub.getTransaction().setCompositeStoreTransaction(compositeTx);
                        try {
                            delay = Math.max(delay, sub.commit());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        sub.getTransaction().setCompositeStoreTransaction(null);
                        sub.getTransaction().unlockQueue(true);
                    }
                    delayCollector.incNumberCallbacks();
                    AsyncCompletionCallback cb = new AsyncCompletionCallback() {
                        public void done(boolean success) {
                            for (int i = 0; i < size; i++) {
                                TopicSubscriberTransaction sub = durPersSubs.get(i);
                                sub.getTransaction().unmarkAsyncActive();
                            }
                            delayCollector.done(this, success);
                        }
                    };
                    cb.setResult(Long.valueOf(delay));
                    compositeTx.commitTransaction(cb);
                }
            }
            subscriberTransactions = null;
        }
        delayCollector.setBlocked(false);
    }

    protected void rollback() throws Exception {
        if (subscriberTransactions == null)
            return;
        List<TopicSubscriberTransaction> durPersSubs = new ArrayList<>(subscriberTransactions.length);
        for (TopicSubscriberTransaction sub : subscriberTransactions) {
            if (sub != null) {
                TopicSubscription subscription = sub.getTopicSubscription();
                if (subscription.isRemote() || !subscription.isDurable() || !sub.isPersistentMessageIncluded()) {
                    try {
                        sub.rollback();
                    } catch (QueueTransactionClosedException ignored) {
                    }
                } else
                    durPersSubs.add(sub);
            }
        }
        int size = durPersSubs.size();
        if (size > 0) {
            if (size == 1) {
                TopicSubscriberTransaction sub = durPersSubs.get(0);
                try {
                    sub.rollback();
                } catch (QueueTransactionClosedException ignored) {
                }
            } else {
                for (int i = 0; i < size; i++) {
                    TopicSubscriberTransaction sub = durPersSubs.get(i);
                    sub.getTransaction().lockQueue();
                }
                CompositeStoreTransaction compositeTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
                for (int i = 0; i < size; i++) {
                    TopicSubscriberTransaction sub = durPersSubs.get(i);
                    try {
                        sub.getTransaction().setCompositeStoreTransaction(compositeTx);
                        sub.rollback();
                        sub.getTransaction().setCompositeStoreTransaction(null);
                    } catch (QueueTransactionClosedException ignored) {
                    }
                }
                compositeTx.commitTransaction();
                for (int i = 0; i < size; i++) {
                    TopicSubscriberTransaction sub = durPersSubs.get(i);
                    sub.getTransaction().unlockQueue(false);
                }
            }
        }
        subscriberTransactions = null;
    }

    protected void rollback(AsyncCompletionCallback callback) {
        final RollbackJoin join = new RollbackJoin(callback);
        if (subscriberTransactions != null) {
            final List<TopicSubscriberTransaction> durPersSubs = new ArrayList<>(subscriberTransactions.length);
            for (TopicSubscriberTransaction sub : subscriberTransactions) {
                if (sub != null) {
                    TopicSubscription subscription = sub.getTopicSubscription();
                    if (subscription.isRemote() || !subscription.isDurable() || !sub.isPersistentMessageIncluded()) {
                        join.incNumberCallbacks();
                        sub.rollback(new RollbackCallback(join));
                    } else
                        durPersSubs.add(sub);
                }
            }
            final int size = durPersSubs.size();
            if (size > 0) {
                if (size == 1) {
                    TopicSubscriberTransaction sub = durPersSubs.get(0);
                    join.incNumberCallbacks();
                    sub.rollback(new RollbackCallback(join));
                } else {
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        sub.getTransaction().lockQueue();
                    }
                    CompositeStoreTransaction compositeTx = ctx.storeSwiftlet.createCompositeStoreTransaction();
                    for (int i = 0; i < size; i++) {
                        TopicSubscriberTransaction sub = durPersSubs.get(i);
                        try {
                            sub.getTransaction().setCompositeStoreTransaction(compositeTx);
                            sub.rollback();
                            sub.getTransaction().setCompositeStoreTransaction(null);
                            sub.getTransaction().unlockQueue(true);
                        } catch (Exception ignored) {
                        }
                    }
                    join.incNumberCallbacks();
                    compositeTx.commitTransaction(new AsyncCompletionCallback() {
                        public void done(boolean success) {
                            for (int i = 0; i < size; i++) {
                                TopicSubscriberTransaction sub = durPersSubs.get(i);
                                sub.getTransaction().unmarkAsyncActive();
                            }
                            join.done(this, success);
                        }
                    });
                }
            }
            subscriberTransactions = null;
        }
        join.setBlocked(false);
    }

    public String toString() {
        return "[TopicTransaction, id=" + transactionId + "]";
    }

    private static class DelayCollector extends CallbackJoin {
        long delay = 0;

        protected DelayCollector(AsyncCompletionCallback asyncCompletionCallback) {
            super(asyncCompletionCallback);
        }

        protected void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last) {
            if (success) {
                Long res = (Long) callback.getResult();
                if (res != null)
                    delay = Math.max(delay, res);
                if (last) {
                    finalResult = delay;
                }
            } else {
                finalSuccess = false;
                finalException = callback.getException();
            }
        }
    }

    private static class RollbackJoin extends CallbackJoin {
        protected RollbackJoin(AsyncCompletionCallback asyncCompletionCallback) {
            super(asyncCompletionCallback);
        }

        protected void callbackDone(AsyncCompletionCallback callback, boolean success, boolean last) {
            if (!success) {
                finalSuccess = false;
                finalException = callback.getException();
            }
        }
    }

    private static class RollbackCallback extends AsyncCompletionCallback {
        RollbackJoin join = null;

        private RollbackCallback(RollbackJoin join) {
            this.join = join;
        }

        public void done(boolean success) {
            join.done(this, success);
        }
    }
}

