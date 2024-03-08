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

import com.swiftmq.impl.queue.standard.QueueManagerImpl;
import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyWatchListener;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.store.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ExpandableList;
import com.swiftmq.tools.collection.OrderedSet;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterLong;
import com.swiftmq.util.SwiftUtilities;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageQueue extends AbstractQueue {
    SwiftletContext ctx = null;
    Cache cache = null;
    PersistentStore pStore = null;
    NonPersistentStore nStore = null;
    protected SortedSet<StoreId> queueContent = null;
    protected OrderedSet duplicateBacklog = new OrderedSet(500);
    volatile boolean running = false;
    ExpandableList<View> views = null;
    AtomicWrappingCounterLong msgId = new AtomicWrappingCounterLong(0);
    boolean alwaysDeliverExpired = false;
    boolean getWaiting = false;
    Selector currentGetSelector = null;
    protected boolean duplicateDetectionEnabled = true;
    protected int duplicateDetectionBacklogSize = 500;
    Lock queueLock = new ReentrantLock();
    Condition msgAvail = queueLock.newCondition();
    Condition asyncFinished = queueLock.newCondition();
    boolean asyncActive = false;
    CompositeStoreTransaction compositeTx = null;
    long activeReceiverId = -1;
    boolean active = true;
    private final QueueLatency queueLatency = new QueueLatency();
    private final QueueMetrics queueMetrics = new QueueMetrics();
    private final WireTapManager wireTapManager = new WireTapManager();
    private final PropertyWatchManager propertyWatchManager = new PropertyWatchManager();
    private final ActiveTransactionRegistry activeTransactionRegistry = new ActiveTransactionRegistry();
    private final MessageProcessorRegistry messageProcessorRegistry = new MessageProcessorRegistry();

    public MessageQueue(SwiftletContext ctx, Cache cache, PersistentStore pStore, NonPersistentStore nStore, long cleanUpDelay) {
        this.ctx = ctx;
        this.cache = cache;
        this.pStore = pStore;
        this.nStore = nStore;
        this.cleanUpInterval = cleanUpDelay;
    }

    private void lockAndWaitAsyncFinished() {
        queueLock.lock();
        while (asyncActive)
            asyncFinished.awaitUninterruptibly();
    }

    @Override
    public void addWireTapSubscriber(String name, WireTapSubscriber subscriber) {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "addWireTapSubscriber, name=" + name + ", subscriber=" + subscriber);
            wireTapManager.addWireTapSubscriber(name, subscriber);
        } finally {
            queueLock.unlock();
        }
    }

    @Override
    public void removeWireTapSubscriber(String name, WireTapSubscriber subscriber) {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "removeWireTapSubscriber, name=" + name + ", subscriber=" + subscriber);
            wireTapManager.removeWireTapSubscriber(name, subscriber);
        } finally {
            queueLock.unlock();
        }
    }

    private void forwardWireTaps(MessageImpl message) {
        wireTapManager.forwardWireTaps(message);
    }

    private void setupPropertyWatch(Entity queueController, String propertyName, PropertyWatchListener listener) {
        Property prop = queueController.getProperty(propertyName);
        prop.addPropertyWatchListener(listener);
        propertyWatchManager.addPropertyWatchListener(prop, listener);
    }

    void setQueueController(Entity queueController) {
        setupPropertyWatch(queueController, QueueManagerImpl.PROP_CACHE_SIZE,
                property -> cache.setMaxMessages((Integer) property.getValue()));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_CACHE_SIZE_BYTES_KB,
                property -> cache.setMaxBytesKB((Integer) property.getValue()));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_MESSAGES_MAXIMUM,
                property -> setMaxMessages((Integer) property.getValue()));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_PERSISTENCE,
                property -> setPersistenceMode(SwiftUtilities.persistenceModeToInt((String) property.getValue())));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_FLOWCONTROL_QUEUE_SIZE,
                property -> {
                    int fcQueueSize = (Integer) property.getValue();
                    if (fcQueueSize >= 0)
                        setFlowController(new FlowControllerImpl(fcQueueSize, ctx.queueManager.getMaxFlowControlDelay()));
                    else
                        setFlowController(null);
                });

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_DUPLICATE_DETECTION_ENABLED,
                property -> setDuplicateDetectionEnabled((Boolean) property.getValue()));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_DUPLICATE_DETECTION_BACKLOG_SIZE,
                property -> setDuplicateDetectionBacklogSize((Integer) property.getValue()));

        setupPropertyWatch(queueController, QueueManagerImpl.PROP_CONSUMER,
                property -> setConsumerMode(ctx.consumerModeInt((String) property.getValue())));
    }

    protected long getNextMsgId() {
        return msgId.getAndIncrement();
    }

    public void setAlwaysDeliverExpired(boolean alwaysDeliverExpired) {
        this.alwaysDeliverExpired = alwaysDeliverExpired;
    }

    public Cache getCache() {
        return cache;
    }

    public int getCacheSize() {
        return cache.getMaxMessages();
    }

    public int getCacheSizeKB() {
        return cache.getMaxBytesKB();
    }

    public int getCurrentCacheSizeMessages() {
        return cache.getCurrentMessages();
    }

    public int getCurrentCacheSizeKB() {
        return cache.getCurrentBytesKB();
    }

    protected void setDuplicateDetectionEnabled(boolean duplicateDetectionEnabled) {
        lockAndWaitAsyncFinished();
        try {
            if (duplicateDetectionEnabled && !this.duplicateDetectionEnabled)
                buildDuplicateBacklog();
            this.duplicateDetectionEnabled = duplicateDetectionEnabled;
            if (!duplicateDetectionEnabled)
                duplicateBacklog.clear();
        } finally {
            queueLock.unlock();
        }
    }

    protected void setDuplicateDetectionBacklogSize(int duplicateDetectionBacklogSize) {
        lockAndWaitAsyncFinished();
        try {
            this.duplicateDetectionBacklogSize = duplicateDetectionBacklogSize;
            duplicateBacklog.resize(Math.max(duplicateDetectionBacklogSize, 0));
        } finally {
            queueLock.unlock();
        }
    }

    public void setFlowController(FlowController flowController) {
        lockAndWaitAsyncFinished();
        try {
            super.setFlowController(flowController);
            if (flowController != null && queueContent != null)
                flowController.setQueueSize(queueContent.size());
        } finally {
            queueLock.unlock();
        }
    }

    protected void buildDuplicateBacklog() {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "buildDuplicateBacklog, queueContent.size=" + queueContent.size() + ", duplicateDetectionBacklogSize=" + duplicateDetectionBacklogSize);
        duplicateBacklog.clear();
        int forwardSpool = Math.max(queueContent.size() - duplicateDetectionBacklogSize, 0);
        int n = 0;
        for (Iterator<StoreId> iter = queueContent.iterator(); iter.hasNext(); n++) {
            StoreId storeId = iter.next();
            if (n >= forwardSpool) {
                try {
                    MessageEntry me = cache.get(storeId);
                    String jmsMsgId = me.getMessage().getJMSMessageID();
                    if (jmsMsgId != null)
                        duplicateBacklog.add(jmsMsgId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "buildDuplicateBacklog done, size=" + duplicateBacklog.size());
    }

    protected void insertDuplicateBacklog(String jmsMsgId, StoreId storeId) {
        duplicateBacklog.add(jmsMsgId);
    }

    private boolean isDuplicate(String jmsMsgId) {
        return duplicateBacklog.contains(jmsMsgId);
    }

    private void overwritePersistence(MessageImpl msg) {
        try {
            int dm = msg.getJMSDeliveryMode();
            if (persistenceMode == AbstractQueue.PERSISTENT &&
                    dm == DeliveryMode.NON_PERSISTENT) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "overwritePersistence to PERSISTENT, Message: " + msg);
                msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            } else if (persistenceMode == AbstractQueue.NON_PERSISTENT &&
                    dm == DeliveryMode.PERSISTENT) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "overwritePersistence to NON_PERSISTENT, Message: " + msg);
                msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
        } catch (Exception ignored) {
        }
    }

    private StoreWriteTransaction insertMessage(StoreWriteTransaction swt, MessageImpl message) throws Exception {
        return insertMessage(swt, message, null);
    }

    private StoreWriteTransaction insertMessage(StoreWriteTransaction swt, MessageImpl message, List<StoreId> preparedList) throws Exception {
        StoreWriteTransaction transaction = swt;
        if (!temporary && persistenceMode != AbstractQueue.AS_MESSAGE)
            overwritePersistence(message);
        StoreId storeId = new StoreId(getNextMsgId(), MessageImpl.MAX_PRIORITY - message.getJMSPriority(), 1, message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT, message.getJMSExpiration(), null);
        if (message.getJMSTimestamp() > 0)
            storeId.setEntryTime(message.getJMSTimestamp());

        // Don't write to disk for temp. queues
        if (temporary)
            storeId.setPersistent(false);
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "insertMessage, StoreId: " + storeId + " Message: " + message);

        // put persistent messages into the backstore
        if (storeId.isPersistent()) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "insertMessage, StoreId: " + storeId + " Message is PERSISTENT");
            if (transaction == null && pStore != null)
                transaction = pStore.createWriteTransaction();
            StoreEntry se = new StoreEntry();
            se.priority = storeId.getPriority();
            se.deliveryCount = storeId.getDeliveryCount();
            se.expirationTime = storeId.getExpirationTime();
            se.message = message;
            transaction.insert(se);
            se.message = null;
            storeId.setPersistentKey(se);
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "insertMessage, StoreId: " + storeId + " Message added to BackStore");
        }

        // Set the key in the message (used vom Streams)
        message.setStreamPKey(storeId);

        // insert storeId into the queueContent
        queueContent.add(storeId);

        // eventually log the storeId in the preparedList and lock it (till commit)
        if (preparedList != null) {
            storeId.setLocked(true);
            preparedList.add(storeId);
        }

        // Forward to WireTaps
        forwardWireTaps(message);

        // put message into the cache
        cache.put(storeId, message);
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "insertMessage, StoreId: " + storeId + " Message added to Cache");

        if (duplicateDetectionEnabled) {
            String jmsMsgId = message.getJMSMessageID();
            if (jmsMsgId != null)
                insertDuplicateBacklog(jmsMsgId, storeId);
        }

        // update views
        if (views != null) {
            insertIntoViews(storeId, message);
        }
        queueMetrics.incrementProduced();
        return transaction;
    }

    private void insertIntoViews(StoreId storeId, MessageImpl message) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "insertIntoViews, StoreId: " + storeId + " Update Views");
        for (int i = 0; i < views.size(); i++) {
            View view = views.get(i);
            if (view != null)
                view.storeOnMatch(storeId, message);
        }
    }

    private StoreReadTransaction removeMessage(StoreReadTransaction srt, StoreId storeId) throws Exception {
        return removeMessage(srt, storeId, false);
    }

    private StoreReadTransaction removeMessage(StoreReadTransaction srt, StoreId storeId, boolean prepare) throws Exception {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "removeMessage, StoreId: " + storeId);

        StoreReadTransaction transaction = srt;

        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "removeMessage, StoreId: " + storeId + " Message removed from Cache");

        // remove persistent messages from the backstore
        if (storeId.isPersistent()) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "removeMessage, StoreId: " + storeId + " Message is PERSISTENT");
            if (transaction == null && pStore != null)
                transaction = pStore.createReadTransaction(true);
            transaction.remove(((StoreEntry) storeId.getPersistentKey()).key);
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "removeMessage, StoreId: " + storeId + " Message removed from BackStore");
        }

        if (!prepare) {
            incrementConsumeCount();

            // remove storeId from the queueContent
            queueContent.remove(storeId);

            // remove message from the cache
            cache.remove(storeId);

            // update views
            if (views != null) {
                removeFromViews(storeId);
            }
        }
        queueLatency.addLatency(storeId.getLatency(System.currentTimeMillis()));

        return transaction;
    }

    private void incrementConsumeCount() {
        queueMetrics.incrementConsumed();
    }

    private void removeFromViews(StoreId storeId) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "removeFromViews, StoreId: " + storeId + " Update Views");
        for (int i = 0; i < views.size(); i++) {
            View view = (View) views.get(i);
            if (view != null)
                view.remove(storeId);
        }
    }

    private Iterator<StoreId> getIterator(int viewId) throws QueueException {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "getIterator, viewId: " + viewId);
        Iterator<StoreId> iterator = null;
        if (viewId == -1)
            iterator = queueContent.iterator();
        else {
            if (views == null || viewId < 0 || viewId > views.size() - 1) {
                throw new QueueException("View with Id " + viewId + " not found!");
            }
            View view = (View) views.get(viewId);
            if (view == null) {
                throw new QueueException("View with Id " + viewId + " not found!");
            }
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "getIterator, view: " + view);
            iterator = view.getViewContent().iterator();
            view.setDirty(false);
        }
        return iterator;
    }

    // Must own the queueSemaphore's monitor
    private void notifyWaiters() {
        if (!active) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "notifyWaiters, Queue is paused");
            return; // Queue is paused
        }
        messageProcessorRegistry.process(mp -> {
            mp.setRegistrationId(-1);
            int viewId = mp.getViewId();
            if (viewId == -1 || views.get(viewId) == null || views.get(viewId).isDirty()) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "notifyWaiters, no view or view is dirty, run message proc");
                registerMessageProcessor(mp);
            } else {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "notifyWaiters, view is NOT dirty, store message proc");
                storeMessageProcessor(mp);
            }
        });
        msgAvail.signalAll();
    }

    private void ensureTxList(TransactionId tId) {
        activeTransactionRegistry.ensureTransaction(tId);
    }

    private void removeTxId(TransactionId tId) {
        int txId = tId.getTxId();
        if (txId != -1 && tId.getTransactionType() == TransactionId.PULL_TRANSACTION) {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "removeTxId: " + txId);
            activeTransactionRegistry.removeTransaction(txId);
        }
    }

    private void clearTransaction(TransactionId transactionId) {
        removeTxId(transactionId);
        transactionId.clear();
    }

    private void buildIndex() throws Exception {
        // build the index out of the PersistentStore
        List<StoreEntry> list = pStore.getStoreEntries();
        for (StoreEntry o : list) {
            queueContent.add(new StoreId(getNextMsgId(), o.priority, o.deliveryCount, true, o.expirationTime, o));
        }
    }

    public void startQueue()
            throws QueueException {
        if (running)
            throw new QueueException("queue is alrady running");

        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "startQueue: PersistentStore=" + pStore);
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "startQueue: NonPersistentStore=" + nStore);
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "startQueue: Cache=" + cache);
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "startQueue: cleanUpInterval=" + cleanUpInterval);
            queueContent = new TreeSet<>();
            try {
                if (!temporary) {
                    buildIndex();
                    if (duplicateDetectionEnabled)
                        buildDuplicateBacklog();
                }
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "Queue has " + queueContent.size() + " entries");
                running = true;
            } catch (Exception e) {
                ctx.logSwiftlet.logError(getQueueName(), "Exception buildIndex: " + e.getMessage());
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "Exception buildIndex: " + e);
                throw new QueueException("Exception buildIndex: " + e);
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void stopQueue()
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "stopQueue ...");
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");

            running = false;
            wireTapManager.reset();
            propertyWatchManager.removePropertyWatchListeners();

            if (isTemporary()) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "stopQueue: queue is temporary, deleting content");
                deleteContent();
            }
            cache.clear();
            try {
                if (pStore != null)
                    pStore.close();
                nStore.close();
            } catch (Exception e) {
                throw new QueueException(e.toString());
            }
            // notifyWaiters();
            messageProcessorRegistry.reset();
            msgAvail.signalAll();
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "stopQueue: queue is stopped");
        } finally {
            queueLock.unlock();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public Object createPushTransaction()
            throws QueueException {
        if (!running)
            throw new QueueException("queue " + getQueueName() + " is not running");
        return new TransactionId(TransactionId.PUSH_TRANSACTION);
    }

    public Object createPullTransaction()
            throws QueueException {
        if (!running)
            throw new QueueException("queue " + getQueueName() + " is not running");
        TransactionId tId = new TransactionId();
        ensureTxList(tId);
        return tId;
    }

    public int createView(Selector selector) {
        lockAndWaitAsyncFinished();
        try {
            if (views == null)
                views = new ExpandableList<>();
            View view = new View(ctx, getQueueName(), -1, selector);
            int viewId = views.add(view);
            view.setViewId(viewId);
            // Fill view
            try {
                for (StoreId storeId : queueContent) {
                    MessageEntry me = cache.get(storeId);
                    view.storeOnMatch(storeId, me.getMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return viewId;
        } finally {
            queueLock.unlock();
        }
    }

    public void deleteView(int viewId) {
        lockAndWaitAsyncFinished();
        try {
            View view = views.get(viewId);
            if (view != null) {
                view.close();
                views.remove(viewId);
            }
        } finally {
            queueLock.unlock();
        }
    }

    private List<StoreId> buildStoreIdList(List persistentKeyList) {
        List<StoreId> list = new ArrayList<>();
        for (Object pk : persistentKeyList) {
            for (StoreId storeId : queueContent) {
                if (storeId.isPersistent() && storeId.getPersistentKey() != null && ((StoreEntry) storeId.getPersistentKey()).key.equals(pk)) {
                    storeId.setLocked(true);
                    list.add(storeId);
                }
            }
        }
        return list;
    }

    public Object buildPreparedTransaction(PrepareLogRecord record) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "buildPreparedTransaction, record: " + record);
            TransactionId transactionId = new TransactionId(record.getType() == PrepareLogRecord.WRITE_TRANSACTION ? TransactionId.PUSH_TRANSACTION : TransactionId.PULL_TRANSACTION);
            transactionId.setAlreadyStored(true);
            transactionId.setPrepared(true);
            transactionId.setLogRecord(record);
            transactionId.setPreparedList(buildStoreIdList(record.getKeyList()));
            return transactionId;
        } finally {
            queueLock.unlock();
        }
    }

    protected void beforeTransactionComplete() {

    }

    public void prepare(Object localTxId, XidImpl globalTxId) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            TransactionId transactionId = (TransactionId) localTxId;
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "prepare: " + transactionId + ", globalTxId: " + globalTxId);
            try {
                StoreTransaction storeTransaction = null;
                List preparedList = new ArrayList();
                List txList = transactionId.getTxList();
                if (txList != null && !txList.isEmpty()) {
                    if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION) {
                        if (maxMessages > 0 && queueContent.size() + txList.size() > maxMessages) {
                            throw new QueueLimitException("Maximum Messages in Queue reached!");
                        }
                        for (Object o : txList) {
                            MessageImpl message = (MessageImpl) o;
                            if (checkDuplicate(message)) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "prepare: " + transactionId + ", duplicate message rejected: " + message);
                                if (ctx.queueManager.isLogDuplicates())
                                    ctx.logSwiftlet.logWarning(getQueueName(), "prepare: " + transactionId + ", duplicate message rejected: " + message);
                            } else
                                storeTransaction = insertMessage((StoreWriteTransaction) storeTransaction, message, preparedList);
                        }
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "prepare: " + transactionId + " SUCCESSFUL");
                    } else { //PULL_TRANSACTION
                        for (Object o : txList)
                            storeTransaction = removeMessage((StoreReadTransaction) storeTransaction, (StoreId) o, true);
                        preparedList.addAll(txList);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "prepare: " + transactionId + " SUCCESSFUL");
                    }
                    txList.clear();
                }
                beforeTransactionComplete();
                if (storeTransaction != null)
                    storeTransaction.prepare(globalTxId);
                transactionId.setPrepared(true);
                transactionId.setStoreTransaction(storeTransaction);
                transactionId.setGlobalTxId(globalTxId);
                transactionId.setPreparedList(preparedList);
            } catch (QueueException e) {
                throw e;
            } catch (Exception e1) {
                throw new QueueException(e1.toString());
            }
        } finally {
            queueLock.unlock();
        }
    }

    private boolean checkDuplicate(MessageImpl message)
            throws JMSException {
        return duplicateDetectionEnabled &&
                message.getBooleanProperty(MessageImpl.PROP_DOUBT_DUPLICATE) &&
                message.getJMSMessageID() != null &&
                isDuplicate(message.getJMSMessageID());
    }

    public void commit(Object localTxId, XidImpl globalTxId) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "commit: " + localTxId + ", globalTxId: " + globalTxId);
            TransactionId transactionId = (TransactionId) localTxId;
            int type = transactionId.getTransactionType();
            if (transactionId.isAlreadyStored()) {
                try {
                    StoreTransaction storeTransaction = null;
                    List preparedList = transactionId.getPreparedList();
                    for (Object o : preparedList) {
                        StoreId storeId = (StoreId) o;
                        storeId.setLocked(false);
                        if (type == TransactionId.PULL_TRANSACTION) {
                            storeTransaction = removeMessage((StoreReadTransaction) storeTransaction, storeId);
                        }
                    }
                    beforeTransactionComplete();
                    if (storeTransaction != null)
                        storeTransaction.commit();
                    ctx.storeSwiftlet.removePrepareLogRecord(transactionId.getLogRecord());
                } catch (Exception e) {
                    throw new QueueException(e.toString());
                }
                clearTransaction(transactionId);
                notifyWaiters();
            } else {
                // Use one phase when not prepared
                if (!transactionId.isPrepared())
                    commit(localTxId);
                else {
                    try {
                        beforeTransactionComplete();
                        StoreTransaction st = transactionId.getStoreTransaction();
                        if (st != null)
                            st.commit(globalTxId);
                        List preparedList = transactionId.getPreparedList();
                        for (Object o : preparedList) {
                            StoreId storeId = (StoreId) o;
                            storeId.setLocked(false);
                            if (type == TransactionId.PULL_TRANSACTION) {
                                queueContent.remove(storeId);
                                if (views != null)
                                    removeFromViews(storeId);
                                cache.remove(storeId);
                                queueMetrics.incrementConsumed();
                            }
                        }
                        if (getFlowController() != null) {
                            if (type == TransactionId.PULL_TRANSACTION)
                                getFlowController().setReceiveMessageCount(preparedList.size());
                            else
                                getFlowController().setSentMessageCount(preparedList.size());
                        }
                        preparedList.clear();
                    } catch (Exception e) {
                        throw new QueueException(e.toString());
                    }
                    clearTransaction(transactionId);

                    if (type == TransactionId.PUSH_TRANSACTION)
                        notifyWaiters();
                }
            }
            if (getFlowController() != null)
                getFlowController().setQueueSize(queueContent.size());
        } finally {
            queueLock.unlock();
        }
    }

    public void commit(Object tId)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            TransactionId transactionId = (TransactionId) tId;
            List txList = transactionId.getTxList();
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId);
            try {
                StoreTransaction storeTransaction = compositeTx;
                if (txList != null && !txList.isEmpty()) {
                    if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION) {
                        if (maxMessages > 0 && queueContent.size() + txList.size() > maxMessages) {
                            throw new QueueLimitException("Maximum Messages in Queue reached!");
                        }
                        for (Object o : txList) {
                            MessageImpl message = (MessageImpl) o;
                            if (checkDuplicate(message)) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + ", duplicate message rejected: " + message);
                                if (ctx.queueManager.isLogDuplicates())
                                    ctx.logSwiftlet.logWarning(getQueueName(), "commit: " + transactionId + ", duplicate message rejected: " + message);
                            } else
                                storeTransaction = insertMessage((StoreWriteTransaction) storeTransaction, message);
                        }
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + " SUCCESSFUL");
                        if (getFlowController() != null)
                            getFlowController().setSentMessageCount(txList.size());
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + " queueSemaphore.notify()");
                    } else { //PULL_TRANSACTION
                        for (Object o : txList)
                            storeTransaction = removeMessage((StoreReadTransaction) storeTransaction, (StoreId) o);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + " SUCCESSFUL");
                        if (getFlowController() != null)
                            getFlowController().setReceiveMessageCount(txList.size());
                    }
                }
                beforeTransactionComplete();
                if (storeTransaction != null && compositeTx == null)
                    storeTransaction.commit();
                clearTransaction(transactionId);
                // notify waiting get's
                if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION)
                    notifyWaiters();
            } catch (QueueException e) {
                throw e;
            } catch (Exception e1) {
                throw new QueueException(e1.toString());
            }
            if (getFlowController() != null)
                getFlowController().setQueueSize(queueContent.size());
        } finally {
            queueLock.unlock();
        }
    }

    public void commit(Object tId, AsyncCompletionCallback callback) {
        lockAndWaitAsyncFinished();
        try {
            if (!running) {
                callback.setException(new QueueException("queue " + getQueueName() + " is not running"));
                callback.notifyCallbackStack(false);
                return;
            }
            final TransactionId transactionId = (TransactionId) tId;
            List txList = transactionId.getTxList();
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId);
            try {
                StoreTransaction storeTransaction = compositeTx;
                if (txList != null && !txList.isEmpty()) {
                    if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION) {
                        if (maxMessages > 0 && queueContent.size() + txList.size() > maxMessages) {
                            throw new QueueLimitException("Maximum Messages in Queue reached!");
                        }
                        for (Object o : txList) {
                            MessageImpl message = (MessageImpl) o;
                            if (checkDuplicate(message)) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + ", duplicate message rejected: " + message);
                                if (ctx.queueManager.isLogDuplicates())
                                    ctx.logSwiftlet.logWarning(getQueueName(), "commit: " + transactionId + ", duplicate message rejected: " + message);
                            } else
                                storeTransaction = insertMessage((StoreWriteTransaction) storeTransaction, message);
                        }
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + " SUCCESSFUL");
                        if (getFlowController() != null)
                            getFlowController().setSentMessageCount(txList.size());
                    } else { //PULL_TRANSACTION
                        for (Object o : txList)
                            storeTransaction = removeMessage((StoreReadTransaction) storeTransaction, (StoreId) o);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "commit: " + transactionId + " SUCCESSFUL");
                        if (getFlowController() != null)
                            getFlowController().setReceiveMessageCount(txList.size());
                        removeTxId(transactionId);
                    }
                }
                beforeTransactionComplete();
                if (storeTransaction != null && compositeTx == null) {
                    asyncActive = true;
                    storeTransaction.commit(new AsyncCompletionCallback(callback) {
                        public void done(boolean success) {
                            queueLock.lock();
                            try {
                                transactionId.clear();
                                if (getFlowController() != null) {
                                    getFlowController().setQueueSize(queueContent.size());
                                    if (success)
                                        next.setResult(getFlowController().getNewDelay());
                                    else
                                        next.setException(getException());
                                }
                            } finally {
                                asyncActive = false;
                                asyncFinished.signalAll();
                                // notify waiting get's
                                if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION)
                                    notifyWaiters();
                                queueLock.unlock();
                            }
                        }
                    });
                } else {
                    transactionId.clear();
                    // notify waiting get's
                    if (transactionId.getTransactionType() == TransactionId.PUSH_TRANSACTION)
                        notifyWaiters();
                    if (getFlowController() != null) {
                        getFlowController().setQueueSize(queueContent.size());
                        callback.setResult(getFlowController().getNewDelay());
                    }
                    callback.notifyCallbackStack(true);
                }
            } catch (Exception e) {
                removeTxId(transactionId);
                callback.setException(e);
                callback.notifyCallbackStack(false);
                asyncActive = false;
                asyncFinished.signalAll();
                return;
            }
        } finally {
            queueLock.unlock();
        }
    }

    private void removeFromDuplicateLog(StoreId storeId) throws Exception {
        MessageEntry me = cache.get(storeId);
        String jmsMsgId = me.getMessage().getJMSMessageID();
        if (jmsMsgId != null)
            removeFromDuplicateLog(jmsMsgId, storeId);
    }

    protected void removeFromDuplicateLog(String jmsMsgId, StoreId storeId) {
        duplicateBacklog.remove(jmsMsgId);
    }

    public void rollback(Object localTxId, XidImpl globalTxId, boolean setRedelivered) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "rollback: " + localTxId + ", globalTxId: " + globalTxId);
            TransactionId transactionId = (TransactionId) localTxId;
            int type = transactionId.getTransactionType();
            if (transactionId.isAlreadyStored()) {
                try {
                    StoreTransaction storeTransaction = null;
                    List preparedList = transactionId.getPreparedList();
                    for (Object o : preparedList) {
                        StoreId storeId = (StoreId) o;
                        storeId.setLocked(false);
                        if (type == TransactionId.PUSH_TRANSACTION) {
                            if (duplicateDetectionEnabled)
                                removeFromDuplicateLog(storeId);
                            storeTransaction = removeMessage((StoreReadTransaction) storeTransaction, storeId);
                        }
                    }
                    beforeTransactionComplete();
                    if (storeTransaction != null)
                        storeTransaction.commit();
                    ctx.storeSwiftlet.removePrepareLogRecord(transactionId.getLogRecord());
                } catch (Exception e) {
                    throw new QueueException(e.toString());
                }
                clearTransaction(transactionId);
                notifyWaiters();
            } else {
                // Not prepared, so just do a local rollback
                if (!transactionId.isPrepared())
                    rollback(localTxId, setRedelivered);
                else {
                    try {
                        // Transaction was already prepared
                        StoreTransaction st = transactionId.getStoreTransaction();
                        List preparedList = transactionId.getPreparedList();
                        if (type == TransactionId.PUSH_TRANSACTION) {
                            // Remove storeId and message from the queue
                            for (Object o : preparedList) {
                                StoreId storeId = (StoreId) o;
                                if (duplicateDetectionEnabled)
                                    removeFromDuplicateLog(storeId);
                                queueContent.remove(storeId);
                                if (views != null)
                                    removeFromViews(storeId);
                                cache.remove(storeId);
                            }
                        } else {
                            // PULL_TRANSACTION
                            // Eventually incremend deliveryCount
                            for (Object o : preparedList) {
                                StoreId storeId = (StoreId) o;
                                storeId.setLocked(false);
                                if (setRedelivered) {
                                    storeId.setDeliveryCount(storeId.getDeliveryCount() + 1);
                                    if (!storeId.isPersistent()) {
                                        StoreEntry pk = (StoreEntry) storeId.getPersistentKey();
                                        if (pk != null)
                                            nStore.updateDeliveryCount(pk.key, storeId.getDeliveryCount());
                                    }
                                }
                            }
                        }
                        beforeTransactionComplete();
                        if (st != null)
                            st.abort(globalTxId);
                        // notify others
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "rollback: " + localTxId + ", globalTxId: " + globalTxId + ", notifyWaiters");
                        clearTransaction(transactionId);
                        notifyWaiters();
                    } catch (Exception e) {
                        throw new QueueException(e.toString());
                    }
                }
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void rollback(Object tId, boolean setRedelivered)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            TransactionId transactionId = (TransactionId) tId;
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId);
            try {
                List txList = transactionId.getTxList();
                if (txList != null) {
                    if (transactionId.getTransactionType() == TransactionId.PULL_TRANSACTION) {
                        StoreReadTransaction srt = null;
                        if (pStore != null && setRedelivered)
                            srt = pStore.createReadTransaction(setRedelivered);
                        // Unlock storeId's
                        for (Object o : txList) {
                            StoreId storeId = (StoreId) o;
                            storeId.setLocked(false);
                            if (setRedelivered) {
                                try {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId + ", pStore=" + pStore + ", srt=" + srt + ", setRedelivered: " + storeId);
                                    storeId.setDeliveryCount(storeId.getDeliveryCount() + 1);
                                    if (storeId.isPersistent() && srt != null)
                                        srt.remove(((StoreEntry) storeId.getPersistentKey()).key);
                                    else {
                                        StoreEntry pk = (StoreEntry) storeId.getPersistentKey();
                                        if (pk != null)
                                            nStore.updateDeliveryCount(pk.key, storeId.getDeliveryCount());
                                    }
                                } catch (Exception e) {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "rollback failed: " + transactionId + ", Exception: " + e.getMessage());
                                    throw new QueueException("rollback failed: " + transactionId + ", Exception: " + e.getMessage());
                                }
                            }
                        }
                        beforeTransactionComplete();
                        if (srt != null)
                            srt.abort();
                        // notify others
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId + ", notifyWaiters");
                        notifyWaiters();
                    }
                    clearTransaction(transactionId);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new QueueException(e.toString());
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void rollback(Object tId, boolean setRedelivered, AsyncCompletionCallback callback) {
        lockAndWaitAsyncFinished();
        try {
            if (!running) {
                callback.setException(new QueueException("queue " + getQueueName() + " is not running"));
                callback.notifyCallbackStack(false);
                return;
            }
            final TransactionId transactionId = (TransactionId) tId;
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId);
            try {
                List txList = transactionId.getTxList();
                if (txList != null) {
                    if (transactionId.getTransactionType() == TransactionId.PULL_TRANSACTION) {
                        StoreReadTransaction srt = null;
                        if (pStore != null && setRedelivered)
                            srt = pStore.createReadTransaction(setRedelivered);
                        // Unlock storeId's
                        for (Object o : txList) {
                            StoreId storeId = (StoreId) o;
                            storeId.setLocked(false);
                            if (setRedelivered) {
                                try {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId + ", pStore=" + pStore + ", srt=" + srt + ", setRedelivered: " + storeId);
                                    storeId.setDeliveryCount(storeId.getDeliveryCount() + 1);
                                    if (storeId.isPersistent() && srt != null)
                                        srt.remove(((StoreEntry) storeId.getPersistentKey()).key);
                                    else {
                                        StoreEntry pk = (StoreEntry) storeId.getPersistentKey();
                                        if (pk != null)
                                            nStore.updateDeliveryCount(pk.key, storeId.getDeliveryCount());
                                    }
                                } catch (Exception e) {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "rollback failed: " + transactionId + ", Exception: " + e.getMessage());
                                    throw new QueueException("rollback failed: " + transactionId + ", Exception: " + e.getMessage());
                                }
                            }
                        }
                        beforeTransactionComplete();
                        if (srt != null) {
                            asyncActive = true;
                            srt.abort(new AsyncCompletionCallback(callback) {
                                public void done(boolean success) {
                                    queueLock.lock();
                                    try {
                                        notifyWaiters();
                                        clearTransaction(transactionId);
                                        if (!success)
                                            next.setException(getException());
                                    } finally {
                                        asyncActive = false;
                                        asyncFinished.signalAll();
                                        queueLock.unlock();
                                    }
                                }
                            });
                        } else {
                            // notify others
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "rollback: " + transactionId + ", notifyWaiters");
                            notifyWaiters();
                            clearTransaction(transactionId);
                            callback.notifyCallbackStack(true);
                        }
                    } else {
                        clearTransaction(transactionId);
                        callback.notifyCallbackStack(true);
                    }
                }
            } catch (Exception e) {
                clearTransaction(transactionId);
                asyncActive = false;
                callback.setException(new QueueException(e.toString()));
                callback.notifyCallbackStack(false);
                return;
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void cleanUpExpiredMessages()
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "Begin cleanUp ...");
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            try {
                ArrayList<StoreId> removeIds = new ArrayList<>();
                long actTime = System.currentTimeMillis();
                for (StoreId storeId : queueContent) {
                    if (storeId.getExpirationTime() != 0 && storeId.getExpirationTime() < actTime && !storeId.isLocked())
                        removeIds.add(storeId);
                }
                if (!removeIds.isEmpty()) {
                    for (StoreId removeId : removeIds) {
                        StoreReadTransaction srt = null;
                        if (pStore != null)
                            srt = pStore.createReadTransaction(false);
                        if (ctx.queueManager.isLogExpired())
                            ctx.logSwiftlet.logWarning(getQueueName(), "CleanUp, removing expired message: " + removeId);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "CleanUp, removing expired message: " + removeId);
                        removeMessage(srt, removeId);
                        if (srt != null) {
                            srt.commit();
                        }
                    }
                    removeIds.clear();
                }
            } catch (Exception e) {
                throw new QueueException(e.toString());
            }
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "... end cleanUp");
        } finally {
            queueLock.unlock();
        }
    }

    public long getNumberQueueMessages()
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                return 0;
            return queueContent.size();
        } finally {
            queueLock.unlock();
        }
    }

    public int getConsumingRate() {
        return queueMetrics.getConsumingRate();
    }

    public int getProducingRate() {
        return queueMetrics.getProducingRate();
    }

    public int getConsumedTotal() {
        return queueMetrics.getConsumedTotal();
    }

    public int getProducedTotal() {
        return queueMetrics.getProducedTotal();
    }

    public long getAndResetAverageLatency() {
        lockAndWaitAsyncFinished();
        try {
            long avg = queueLatency.getAverage();
            queueLatency.reset();
            return avg;
        } finally {
            queueLock.unlock();
        }
    }

    public void resetCounters() {
        queueMetrics.resetCounters();
    }

    public MessageEntry getMessage(Object transactionId)
            throws QueueException {
        MessageEntry me = null;
        try {
            me = getMessage(transactionId, null, -1);
        } catch (QueueTimeoutException e) {
        }
        return me;
    }

    public MessageEntry getMessage(Object transactionId, Selector selector)
            throws QueueException {
        MessageEntry me = null;
        try {
            me = getMessage(transactionId, selector, -1);
        } catch (QueueTimeoutException e) {
        }

        return me;
    }

    public MessageEntry getMessage(Object transactionId, long timeout)
            throws QueueException, QueueTimeoutException {
        return getMessage(transactionId, null, timeout);
    }

    public MessageEntry getExpiredMessage(Object tId, long timeout)
            throws QueueException, QueueTimeoutException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            TransactionId transactionId = (TransactionId) tId;
            ensureTxList(transactionId);
            long waitStart = 0;
            if (timeout > 0)
                waitStart = System.currentTimeMillis();
            MessageEntry me = null;
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId);
            StoreId storeId = null;
            while (me == null) {
                List txList = transactionId.getTxList();
                if (txList == null) {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " Invalid transaction (1)");
                    throw new QueueException("Invalid transaction");
                }
                try {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " queueContent.first()");
                    long actTime = System.currentTimeMillis();
                    Iterator<StoreId> iterator = queueContent.iterator();
                    while (iterator.hasNext()) {
                        storeId = iterator.next();
                        long expiration = storeId.getExpirationTime();
                        if (expiration != 0 && expiration < actTime) {
                            if (!storeId.isLocked())
                                break;
                        } else {
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "Not expired, not selected: " + storeId);
                        }
                        storeId = null;
                        me = null;
                    }
                    if (storeId != null) {
                        storeId.setLocked(true);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " StoreId: " + storeId);
                        if (me == null)
                            me = cache.get(storeId);
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " Message: " + me.getMessage());
                    } else {
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " NoSuchElement, queueSemaphore.wait()");
                        if (timeout == 0) {
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " no message, unblocking, return null");
                            break;
                        } else {
                            long timeRest = timeout - (System.currentTimeMillis() - waitStart);
                            if (timeout > 0 && System.currentTimeMillis() - waitStart > timeout) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " no message, timeout, QueueTimeoutException");
                                throw new QueueTimeoutException("timeout occurred");
                            }
                            if (timeout == -1) {
                                try {
                                    msgAvail.await();
                                } catch (Exception ignored) {
                                }
                            } else {
                                try {
                                    msgAvail.await(timeRest, TimeUnit.MILLISECONDS);
                                } catch (Exception ignored) {
                                }
                            }
                        }
                        if (!running)
                            throw new QueueException("queue " + getQueueName() + " is not running");
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " receive NOTIFY");
                    }
                    if (storeId != null) {
                        txList = transactionId.getTxList();
                        if (txList != null) {
                            storeId.setTxId(transactionId.getTxId());
                            txList.add(storeId);
                        } else {
                            storeId.setLocked(false);
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " Invalid transaction (2)");
                            throw new QueueException("Invalid transaction");
                        }
                    }
                } catch (Exception e2) {
                    if (e2 instanceof QueueTimeoutException)
                        throw (QueueTimeoutException) e2;
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "Exception cache.get: " + e2.getMessage());
                    throw new QueueException("Exception cache.get: " + e2.getMessage());
                }
            }
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "getExpiredMessage: " + transactionId + " Returning MessageEntry: " + me);
            return (me);
        } finally {
            queueLock.unlock();
        }
    }

    public MessageEntry getMessage(Object tId, Selector selector, long timeout)
            throws QueueException, QueueTimeoutException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            MessageEntry me;
            try {
                getWaiting = true;
                currentGetSelector = selector;
                TransactionId transactionId = (TransactionId) tId;
                long waitStart = 0;
                if (timeout > 0)
                    waitStart = System.currentTimeMillis();
                me = null;
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "get: " + transactionId);
                StoreId storeId = null;
                while (me == null) {
                    List txList = transactionId.getTxList();
                    if (txList == null) {
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Invalid transaction (1)");
                        throw new QueueException("Invalid transaction");
                    }
                    try {
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " queueContent.first()");
                        long actTime = System.currentTimeMillis();
                        boolean deliverExpired = alwaysDeliverExpired || ctx.queueManager.isDeliverExpired();
                        Iterator<StoreId> iterator = queueContent.iterator();
                        while (iterator.hasNext()) {
                            storeId = iterator.next();
                            long expiration = storeId.getExpirationTime();
                            if (deliverExpired || expiration == 0 || expiration > actTime) {
                                if (selector == null) {
                                    if (!storeId.isLocked())
                                        break;
                                } else {
                                    if (!storeId.isLocked()) {
                                        me = cache.get(storeId);
                                        if (selector.isSelected(me.getMessage())) {
                                            if (ctx.queueSpace.enabled)
                                                ctx.queueSpace.trace(getQueueName(), "selector " + selector + " select Message: " + me.getMessage());
                                            break;
                                        } else {
                                            if (ctx.queueSpace.enabled)
                                                ctx.queueSpace.trace(getQueueName(), "selector " + selector + " DOES NOT select Message: " + me.getMessage());
                                        }
                                    }
                                }
                            } else {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "Expired, not selected: " + storeId);
                            }
                            storeId = null;
                            me = null;
                        }
                        if (storeId != null) {
                            storeId.setLocked(true);
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " StoreId: " + storeId);
                            if (me == null)
                                me = cache.get(storeId);
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Message: " + me.getMessage());
                        } else {
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " NoSuchElement, queueSemaphore.wait(), timeout=" + timeout);
                            if (timeout == 0) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " no message, unblocking, return null");
                                break;
                            } else {
                                long timeRest = timeout - (System.currentTimeMillis() - waitStart);
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " queueSemaphore.wait(), timeRest=" + timeRest);
                                if (timeout > 0 && timeRest <= 0) {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " no message, timeout, QueueTimeoutException");
                                    throw new QueueTimeoutException("timeout occurred");
                                }
                                if (timeout == -1) {
                                    try {
                                        msgAvail.await();
                                    } catch (Exception ignored) {
                                    }
                                } else {
                                    try {
                                        msgAvail.await(timeRest, TimeUnit.MILLISECONDS);
                                    } catch (Exception ignored) {
                                    }
                                }
                            }
                            if (!running)
                                throw new QueueException("queue " + getQueueName() + " is not running");
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " receive NOTIFY");
                        }
                        if (storeId != null) {
                            txList = transactionId.getTxList();
                            if (txList != null) {
                                storeId.setTxId(transactionId.getTxId());
                                txList.add(storeId);
                            } else {
                                storeId.setLocked(false);
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Invalid transaction (2)");
                                throw new QueueException("Invalid transaction");
                            }
                        }
                    } catch (Exception e2) {
                        if (e2 instanceof QueueTimeoutException)
                            throw (QueueTimeoutException) e2;
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "Exception cache.get: " + e2.getMessage());
                        throw new QueueException("Exception cache.get: " + e2.getMessage());
                    }
                }
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Returning MessageEntry: " + me);
            } finally {
                getWaiting = false;
                currentGetSelector = null;
            }
            return (me);
        } finally {
            queueLock.unlock();
        }
    }

    public MessageEntry getMessage(Object transactionId, Selector selector, int viewId) throws QueueException {
        MessageEntry me = null;
        try {
            me = getMessage(transactionId, selector, viewId, -1);
        } catch (QueueTimeoutException e) {
        }

        return me;
    }

    public MessageEntry getMessage(Object tId, Selector selector, int viewId, long timeout) throws QueueException, QueueTimeoutException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            MessageEntry me;
            try {
                getWaiting = true;
                currentGetSelector = selector;
                TransactionId transactionId = (TransactionId) tId;
                long waitStart = 0;
                if (timeout > 0)
                    waitStart = System.currentTimeMillis();
                me = null;
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "get: " + transactionId);
                StoreId storeId = null;
                while (me == null) {
                    List txList = transactionId.getTxList();
                    if (txList == null) {
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Invalid transaction (1)");
                        throw new QueueException("Invalid transaction");
                    }
                    try {
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " queueContent.first()");
                        long actTime = System.currentTimeMillis();
                        boolean deliverExpired = alwaysDeliverExpired || ctx.queueManager.isDeliverExpired();
                        Iterator<StoreId> iterator = getIterator(viewId);
                        while (iterator.hasNext()) {
                            storeId = iterator.next();
                            long expiration = storeId.getExpirationTime();
                            if (deliverExpired || expiration == 0 || expiration > actTime) {
                                if (!storeId.isLocked())
                                    break;
                            } else {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "Expired, not selected: " + storeId);
                            }
                            storeId = null;
                            me = null;
                        }
                        if (storeId != null) {
                            storeId.setLocked(true);
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " StoreId: " + storeId);
                            if (me == null)
                                me = cache.get(storeId);
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Message: " + me.getMessage());
                        } else {
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " NoSuchElement, queueSemaphore.wait(), timeout=" + timeout);
                            if (timeout == 0) {
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " no message, unblocking, return null");
                                break;
                            } else {
                                long timeRest = timeout - (System.currentTimeMillis() - waitStart);
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " queueSemaphore.wait(), timeRest=" + timeRest);
                                if (timeout > 0 && timeRest <= 0) {
                                    if (ctx.queueSpace.enabled)
                                        ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " no message, timeout, QueueTimeoutException");
                                    throw new QueueTimeoutException("timeout occurred");
                                }
                                if (timeout == -1) {
                                    try {
                                        msgAvail.await();
                                    } catch (Exception ignored) {
                                    }
                                } else {
                                    try {
                                        msgAvail.await(timeRest, TimeUnit.MILLISECONDS);
                                    } catch (Exception ignored) {
                                    }
                                }
                            }
                            if (!running)
                                throw new QueueException("queue " + getQueueName() + " is not running");
                            if (ctx.queueSpace.enabled)
                                ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " receive NOTIFY");
                        }
                        if (storeId != null) {
                            txList = transactionId.getTxList();
                            if (txList != null) {
                                storeId.setTxId(transactionId.getTxId());
                                txList.add(storeId);
                            } else {
                                storeId.setLocked(false);
                                if (ctx.queueSpace.enabled)
                                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Invalid transaction (2)");
                                throw new QueueException("Invalid transaction");
                            }
                        }
                    } catch (Exception e2) {
                        if (e2 instanceof QueueTimeoutException)
                            throw (QueueTimeoutException) e2;
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "Exception cache.get: " + e2.getMessage());
                        throw new QueueException("Exception cache.get: " + e2.getMessage());
                    }
                }
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Returning MessageEntry: " + me);
            } finally {
                getWaiting = false;
                currentGetSelector = null;
            }
            return (me);
        } finally {
            queueLock.unlock();
        }
    }

    private int storeMessageProcessor(MessageProcessor messageProcessor) {
        return messageProcessorRegistry.storeMessageProcessor(messageProcessor);
    }

    private void _registerBulkMessageProcessor(MessageProcessor messageProcessor) {
        if (!messageProcessor.isValid())
            return;
        if (!running)
            messageProcessor.processException(new QueueException("queue " + getQueueName() + " is not running"));
        int numberMessages = 0;
        long currentBulkSize = 0;
        TransactionId transactionId = (TransactionId) messageProcessor.getTransactionId();
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "_registerBulkMessageProcessor: " + transactionId + "...");
        List txList = transactionId.getTxList();
        if (txList == null) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "_registerBulkMessageProcessor: " + transactionId + " Invalid transaction (1)");
            messageProcessor.processException(new QueueException("Invalid transaction"));
            return;
        }
        MessageEntry[] bulkBuffer = messageProcessor.getBulkBuffer();
        try {
            long actTime = System.currentTimeMillis();
            boolean deliverExpired = alwaysDeliverExpired || ctx.queueManager.isDeliverExpired();
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "_registerBulkMessageProcessor: " + transactionId + ", deliverExpired: " + deliverExpired + ", alwaysDeliverExpired: " + alwaysDeliverExpired + ", ctx.queueManager.isDeliverExpired(): " + ctx.queueManager.isDeliverExpired());
            Iterator<StoreId> iterator = null;
            int viewId = messageProcessor.getViewId();
            try {
                iterator = getIterator(viewId);
            } catch (QueueException e) {
                messageProcessor.processException(e);
                return;
            }
            List<StoreId> removeIds = null;
            long maxBulkSize = messageProcessor.getMaxBulkSize();
            while (iterator.hasNext() && numberMessages < bulkBuffer.length && (maxBulkSize == -1 || currentBulkSize < maxBulkSize)) {
                StoreId storeId = iterator.next();
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "processing: " + storeId);
                long expiration = storeId.getExpirationTime();
                if (deliverExpired || expiration == 0 || expiration > actTime) {
                    MessageEntry me = null;
                    if (!storeId.isLocked()) {
                        me = cache.get(storeId);
                        if (messageProcessor.isAutoCommit()) {
                            incrementConsumeCount();
                            iterator.remove();
                            if (viewId != -1)
                                queueContent.remove(storeId);
                            if (views != null)
                                removeFromViews(storeId);
                            cache.remove(storeId);
                            if (pStore != null && storeId.isPersistent()) {
                                if (removeIds == null)
                                    removeIds = new ArrayList<StoreId>();
                                removeIds.add(storeId);
                            }
                        } else {
                            storeId.setLocked(true);
                            storeId.setTxId(transactionId.getTxId());
                            txList.add(storeId);
                        }
                        long msize = me.getMessage().getMessageLength();
                        if (msize > 0)
                            currentBulkSize += msize;
                        bulkBuffer[numberMessages++] = me;
                        if (ctx.queueSpace.enabled)
                            ctx.queueSpace.trace(getQueueName(), "# messages in bulk: " + numberMessages + ", message-size=" + msize + ", currentBulkSize=" + currentBulkSize + ", maxBulkSize=" + maxBulkSize + ", bulkBuffer.length=" + bulkBuffer.length + ", iterator.hasNext()=" + iterator.hasNext());
                    }
                } else {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "Expired, not selected: " + storeId);
                }
            }
            if (removeIds != null) {
                StoreReadTransaction srt = pStore.createReadTransaction(false);
                for (StoreId removeId : removeIds)
                    srt.remove(((StoreEntry) removeId.getPersistentKey()).key);
                srt.commit();
                removeIds = null;
            }
        } catch (Exception e) {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "Exception cache.get: " + e.getMessage());
            messageProcessor.processException(new QueueException("Exception cache.get: " + e.getMessage()));
            return;
        }
        if (numberMessages == 0) {
            long timeout = messageProcessor.getTimeout();
            int id = -1;
            if (timeout >= 0)
                id = storeMessageProcessor(messageProcessor);
            if (timeout > 0) {
                long registrationTime = System.currentTimeMillis();
                messageProcessor.setRegistrationTime(registrationTime);
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "registering timeout listener for timeout: " + timeout + ", id: " + id + ", regTime: " + registrationTime);
                ctx.timerSwiftlet.addInstantTimerListener(timeout, new TimeoutProcessor(registrationTime, timeout, id));
            } else if (timeout == -1) // NoWait
            {
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "receiveNoWait, immediate timeout");
                messageProcessor.processException(new QueueTimeoutException("timout occurred"));
            }
            return;
        } else {
            if (getFlowController() != null && messageProcessor.isAutoCommit())
                getFlowController().setReceiveMessageCount(numberMessages);
        }
        messageProcessor.setCurrentBulkSize(currentBulkSize);
        messageProcessor.processMessages(numberMessages);
    }

    private void _registerMessageProcessor(MessageProcessor messageProcessor) {
        if (!messageProcessor.isValid())
            return;
        if (!running)
            messageProcessor.processException(new QueueException("queue " + getQueueName() + " is not running"));
        MessageEntry me = null;
        StoreId storeId = null;
        TransactionId transactionId = (TransactionId) messageProcessor.getTransactionId();
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "_registerMessageProcessor: " + transactionId + "...");
        List txList = transactionId.getTxList();
        if (txList == null) {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "_registerMessageProcessor: " + transactionId + " Invalid transaction (1)");
            messageProcessor.processException(new QueueException("Invalid transaction"));
            return;
        }
        try {
            long actTime = System.currentTimeMillis();
            boolean deliverExpired = alwaysDeliverExpired || ctx.queueManager.isDeliverExpired();
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "_registerMessageProcessor: " + transactionId + ", deliverExpired: " + deliverExpired + ", alwaysDeliverExpired: " + alwaysDeliverExpired + ", ctx.queueManager.isDeliverExpired(): " + ctx.queueManager.isDeliverExpired());
            Iterator<StoreId> iterator = null;
            try {
                iterator = getIterator(messageProcessor.getViewId());
            } catch (QueueException e) {
                messageProcessor.processException(e);
                return;
            }
            while (iterator.hasNext()) {
                storeId = iterator.next();
                long expiration = storeId.getExpirationTime();
                if (deliverExpired || expiration == 0 || expiration > actTime) {
                    if (!storeId.isLocked())
                        break;
                } else {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "Expired, not selected: " + storeId);
                }
                storeId = null;
                me = null;
            }
            if (storeId != null) {
                if (!messageProcessor.isAutoCommit())
                    storeId.setLocked(true);
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "_registerMessageProcessor: StoreId: " + storeId);
                if (me == null)
                    me = cache.get(storeId);
                if (messageProcessor.isAutoCommit()) {
                    incrementConsumeCount();
                    queueContent.remove(storeId);
                    if (views != null)
                        removeFromViews(storeId);
                    cache.remove(storeId);
                    if (pStore != null && storeId.isPersistent()) {
                        StoreReadTransaction srt = pStore.createReadTransaction(false);
                        srt.remove(((StoreEntry) storeId.getPersistentKey()).key);
                        srt.commit();
                    }
                }
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "_registerMessageProcessor: Message: " + me.getMessage());
            }
        } catch (Exception e) {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "Exception cache.get: " + e.getMessage());
            messageProcessor.processException(new QueueException("Exception cache.get: " + e.getMessage()));
            return;
        }
        if (storeId != null) {
            if (!messageProcessor.isAutoCommit()) {
                txList = transactionId.getTxList();
                if (txList != null) {
                    storeId.setTxId(transactionId.getTxId());
                    txList.add(storeId);
                } else {
                    storeId.setLocked(false);
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "get: " + transactionId + " Invalid transaction (2)");
                    messageProcessor.processException(new QueueException("Invalid transaction"));
                    return;
                }
            } else {
                if (getFlowController() != null)
                    getFlowController().setReceiveMessageCount(1);
            }
        } else {
            long timeout = messageProcessor.getTimeout();
            int id = -1;
            if (timeout >= 0)
                id = storeMessageProcessor(messageProcessor);
            if (timeout > 0) {
                long registrationTime = System.currentTimeMillis();
                messageProcessor.setRegistrationTime(registrationTime);
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "registering timeout listener for timeout: " + timeout + ", id: " + id + ", regTime: " + registrationTime);
                ctx.timerSwiftlet.addInstantTimerListener(timeout, new TimeoutProcessor(registrationTime, timeout, id));
            } else if (timeout == -1) // NoWait
            {
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "receiveNoWait, immediate timeout");
                messageProcessor.processException(new QueueTimeoutException("timout occurred"));
            }
            return;
        }
        messageProcessor.processMessage(me);
    }

    public void registerMessageProcessor(MessageProcessor messageProcessor) {
        lockAndWaitAsyncFinished();
        try {
            if (consumerMode == AbstractQueue.ACTIVESTANDBY && activeReceiverId == -1) {
                activeReceiverId = messageProcessor.getReceiverId();
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "registerMessageProcessor, consumerMode is ACTIVESTANDBY and activeReceiverId now set to " + activeReceiverId);
            }
            if (consumerMode == AbstractQueue.ACTIVESTANDBY && activeReceiverId != messageProcessor.getReceiverId()) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "registerMessageProcessor, consumerMode is ACTIVESTANDBY and activeReceiverId (" + activeReceiverId + ") !=  messageProcessor.getReceiverId() (" + messageProcessor.getReceiverId() + ")");
                storeMessageProcessor(messageProcessor);
            } else {
                if (!active) {
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "registerMessageProcessor, Queue is paused, store message processor");
                    storeMessageProcessor(messageProcessor);
                } else {
                    if (messageProcessor.isBulkMode())
                        _registerBulkMessageProcessor(messageProcessor);
                    else
                        _registerMessageProcessor(messageProcessor);
                }
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void unregisterMessageProcessor(MessageProcessor messageProcessor) {
        lockAndWaitAsyncFinished();
        try {
            messageProcessorRegistry.removeMessageProcessor(messageProcessor);
        } finally {
            queueLock.unlock();
        }
    }

    public boolean isActive() {
        lockAndWaitAsyncFinished();
        try {
            return active;
        } finally {
            queueLock.unlock();
        }
    }

    public void activate(boolean b) {
        lockAndWaitAsyncFinished();
        try {
            active = b;
            if (getFlowController() != null)
                ((FlowControllerImpl) getFlowController()).active(b);
            if (active)
                notifyWaiters();
        } finally {
            queueLock.unlock();
        }
    }

    public void setConsumerMode(int consumerMode) {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "setConsumerMode, consumerMode=" + consumerMode);
            super.setConsumerMode(consumerMode);
            activeReceiverId = -1;
            if (isRunning())
                notifyWaiters();
        } finally {
            queueLock.unlock();
        }
    }

    public void receiverClosed(long receiverId) {
        lockAndWaitAsyncFinished();
        try {
            if (consumerMode == AbstractQueue.ACTIVESTANDBY && activeReceiverId == receiverId) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "receiverClosed, receiverId=" + receiverId + ", consumerMode is ACTIVESTANDBY and was active receiver, now -1");
                activeReceiverId = -1;
                notifyWaiters();
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void timeoutMessageProcessor(long registrationTime, int id) {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                return;
            messageProcessorRegistry.removeIf(processor -> processor.getRegistrationTime() == registrationTime);
        } finally {
            queueLock.unlock();
        }
    }

    public void removeMessages(Object tId, List<MessageIndex> messageIndexes)
            throws QueueException {
        lockAndWaitAsyncFinished();
        if (!running)
            throw new QueueException("queue " + getQueueName() + " is not running");
        TransactionId transactionId = (TransactionId) tId;
        List txList = transactionId.getTxList();
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "removeMessages txId=" + transactionId + " messageIndexes=" + messageIndexes);
        try {
            for (MessageIndex messageIndex : messageIndexes) {
                StoreId storeId = (StoreId) messageIndex;
                if (queueContent.contains(storeId)) {
                    if (storeId.isLocked())
                        throw new MessageLockedException("Cannot delete message " + storeId + ". Message is currently locked by a consumer!");

                    storeId.setLocked(true);
                    txList.add(storeId);
                } else
                    throw new QueueException("Cannot delete message " + storeId + ". Message not found!");
            }

            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "removeMessages txId=" + transactionId + " messageIndexes=" + messageIndexes + " SUCCESSFUL");
        } finally {
            queueLock.unlock();
        }
    }

    public void acknowledgeMessage(Object tId, MessageIndex messageIndex)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            TransactionId transactionId = (TransactionId) tId;
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "acknowledgeMessage txId=" + transactionId + " messageIndex=" + messageIndex);
            List txList = transactionId.getTxList();
            try {
                if (txList != null) {
                    StoreId storeId = null;
                    for (Object o : txList) {
                        storeId = (StoreId) o;
                        if (storeId.equals(messageIndex))
                            break;
                        else
                            storeId = null;
                    }
                    if (storeId != null) {
                        StoreReadTransaction srt = null;
                        if (pStore != null && storeId.isPersistent())
                            srt = pStore.createReadTransaction(false);
                        removeMessage(srt, storeId);
                        if (srt != null)
                            srt.commit();
                        txList.remove(storeId);
                        if (getFlowController() != null)
                            getFlowController().setReceiveMessageCount(1);
                    }
                    if (ctx.queueSpace.enabled)
                        ctx.queueSpace.trace(getQueueName(), "acknowledgeMessage txId=" + transactionId + " messageIndex=" + messageIndex + " SUCCESSFUL");
                }
            } catch (Exception e) {
                throw new QueueException(e.toString());
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void acknowledgeMessage(Object tId, MessageIndex messageIndex, AsyncCompletionCallback callback) {
        lockAndWaitAsyncFinished();
        try {
            if (!running) {
                callback.setException(new QueueException("queue " + getQueueName() + " is not running"));
                callback.notifyCallbackStack(false);
                return;
            }
            TransactionId transactionId = (TransactionId) tId;
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "acknowledgeMessage txId=" + transactionId + " messageIndex=" + messageIndex);
            final List txList = transactionId.getTxList();
            try {
                if (txList != null) {
                    StoreId storeId = null;
                    for (Object o : txList) {
                        storeId = (StoreId) o;
                        if (storeId.equals(messageIndex))
                            break;
                        else
                            storeId = null;
                    }
                    if (storeId != null) {
                        final StoreId sid = storeId;
                        callback.setResult(storeId.getMsgSize());
                        txList.remove(sid);
                        if (getFlowController() != null)
                            getFlowController().setReceiveMessageCount(1);
                        StoreReadTransaction srt = null;
                        if (pStore != null && storeId.isPersistent())
                            srt = pStore.createReadTransaction(false);
                        removeMessage(srt, storeId);
                        if (srt != null) {
                            asyncActive = true;
                            srt.commit(new AsyncCompletionCallback(callback) {
                                public void done(boolean b) {
                                    queueLock.lock();
                                    asyncActive = false;
                                    asyncFinished.signalAll();
                                    queueLock.unlock();
                                }
                            });
                        } else
                            callback.notifyCallbackStack(true);

                    } else
                        // May happen that an auto-ack storeId wasn't found after a transparent reconnect
                        // but the client is waiting for a reply...
                        callback.notifyCallbackStack(true);

                }
            } catch (Exception e) {
                callback.setException(new QueueException(e.toString()));
                callback.notifyCallbackStack(false);
                asyncActive = false;
                asyncFinished.signalAll();
                return;
            }
        } finally {
            queueLock.unlock();
        }
    }

    public void acknowledgeMessages(Object tId, List messageIndexList, AsyncCompletionCallback callback) {
        lockAndWaitAsyncFinished();
        try {
            if (!running) {
                callback.setException(new QueueException("queue " + getQueueName() + " is not running"));
                callback.notifyCallbackStack(false);
                return;
            }
            TransactionId transactionId = (TransactionId) tId;
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "acknowledgeMessage txId=" + transactionId + " messageIndexList=" + messageIndexList);
            final List txList = transactionId.getTxList();
            try {
                if (txList != null) {
                    long size = 0;
                    int n = 0;
                    StoreReadTransaction srt = null;
                    for (Object o : messageIndexList) {
                        MessageIndex messageIndex = (MessageIndex) o;
                        for (Iterator iter = txList.iterator(); iter.hasNext(); ) {
                            StoreId storeId = (StoreId) iter.next();
                            if (storeId.equals(messageIndex)) {
                                if (pStore != null && srt == null && storeId.isPersistent())
                                    srt = pStore.createReadTransaction(false);
                                size += storeId.getMsgSize();
                                n++;
                                removeMessage(srt, storeId);
                                iter.remove();
                            }
                        }
                    }
                    callback.setResult(size);
                    if (getFlowController() != null)
                        getFlowController().setReceiveMessageCount(n);
                    if (srt != null) {
                        asyncActive = true;
                        srt.commit(new AsyncCompletionCallback(callback) {
                            public void done(boolean b) {
                                queueLock.lock();
                                asyncActive = false;
                                asyncFinished.signalAll();
                                queueLock.unlock();
                            }
                        });
                    } else {
                        callback.notifyCallbackStack(true);
                    }
                }
            } catch (Exception e) {
                callback.setException(new QueueException(e.toString()));
                callback.notifyCallbackStack(false);
                asyncActive = false;
                asyncFinished.signalAll();
                return;
            }
        } finally {
            queueLock.unlock();
        }
    }

    public long moveToTransactionReturnSize(MessageIndex messageIndex, Object sourceTxId, Object destTxId)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "moveToTransaction messageIndex=" + messageIndex + ", sourceTxId=" + sourceTxId + ", destTxId=" + destTxId);
            long size = 0;
            TransactionId sTxId = (TransactionId) sourceTxId;
            TransactionId dTxId = (TransactionId) destTxId;
            List sTx = sTxId.getTxList();
            List dTx = dTxId.getTxList();
            boolean found = false;
            for (int i = 0; i < sTx.size(); i++) {
                StoreId storeId = (StoreId) sTx.get(i);
                if (storeId.equals(messageIndex)) {
                    dTx.add(storeId);
                    sTx.remove(i);
                    size = storeId.getMsgSize();
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new QueueException("moveToTransaction, messageIndex '" + messageIndex + "' was not found");
            return size;
        } finally {
            queueLock.unlock();
        }
    }

    public void moveToTransaction(MessageIndex messageIndex, Object sourceTxId, Object destTxId)
            throws QueueException {
        moveToTransactionReturnSize(messageIndex, sourceTxId, destTxId);
    }

    public long moveToTransactionReturnSize(MessageIndex messageIndex, Object destTxId) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            int txId = messageIndex.getTxId();
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "moveToTransaction messageIndex=" + messageIndex + "sourceTxId=" + txId + ", destTxId=" + destTxId);
            if (txId == -1) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "moveToTransaction messageIndex=" + messageIndex + ", destTxId=" + destTxId + ": txId not set!");
                throw new QueueException("moveToTransaction messageIndex=" + messageIndex + ", destTxId=" + destTxId + ": txId not set!");
            }
            long size = 0;
            TransactionId dTxId = (TransactionId) destTxId;
            List<StoreId> sTx = activeTransactionRegistry.getTransactionList(txId);
            List<StoreId> dTx = activeTransactionRegistry.getTransactionList(dTxId.getTxId());
            if (ctx.queueSpace.enabled)
                ctx.queueSpace.trace(getQueueName(), "moveToTransaction messageIndex=" + messageIndex + ", sourceTxId=" + txId + ", destTxId=" + dTxId.getTxId());
            boolean found = false;
            for (int i = 0; i < sTx.size(); i++) {
                StoreId storeId = sTx.get(i);
                if (storeId.equals(messageIndex)) {
                    dTx.add(storeId);
                    sTx.remove(i);
                    storeId.setTxId(dTxId.getTxId());
                    size = storeId.getMsgSize();
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (ctx.queueSpace.enabled)
                    ctx.queueSpace.trace(getQueueName(), "moveToTransaction, messageIndex '" + messageIndex + "' was not found");
                throw new QueueException("moveToTransaction, messageIndex '" + messageIndex + "' was not found");
            }
            return size;
        } finally {
            queueLock.unlock();
        }
    }

    public void moveToTransaction(MessageIndex messageIndex, Object destTxId) throws QueueException {
        moveToTransactionReturnSize(messageIndex, destTxId);
    }

    public boolean hasReceiver(MessageImpl message) {
        lockAndWaitAsyncFinished();
        try {
            if (getWaiting && (currentGetSelector == null || currentGetSelector.isSelected(message)))
                return true;
            return messageProcessorRegistry.hasInterestedProcessor(message);
        } finally {
            queueLock.unlock();
        }
    }

    public void putMessage(Object tId, MessageImpl message)
            throws QueueException {
        TransactionId transactionId = (TransactionId) tId;
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "put: " + transactionId);
        List txList = transactionId.getTxList();
        if (txList != null)
            txList.add(message);
    }

    public SortedSet getQueueIndex()
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            return new TreeSet<>(queueContent);
        } finally {
            queueLock.unlock();
        }
    }

    public SortedSet getQueueIndex(int viewId) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (views == null || viewId < 0 || viewId > views.size() - 1)
                throw new QueueException("View with id = " + viewId + " unknown!");
            View view = views.get(viewId);
            if (view == null)
                throw new QueueException("View with id = " + viewId + " unknown!");
            return new TreeSet<>(view.getViewContent());
        } finally {
            queueLock.unlock();
        }
    }

    public MessageEntry getMessageByIndex(MessageIndex messageIndex)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            MessageEntry me = null;
            try {
                if (queueContent.contains(messageIndex))
                    me = cache.get((StoreId) messageIndex);
            } catch (Exception e) {
                throw new QueueException(e.getMessage());
            }
            return me;
        } finally {
            queueLock.unlock();
        }
    }

    public void removeMessageByIndex(MessageIndex messageIndex)
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            try {
                if (queueContent.contains(messageIndex)) {
                    if (((StoreId) messageIndex).isLocked())
                        throw new MessageLockedException("Cannot delete message " + messageIndex + ". Message is currently locked by a consumer!");
                    StoreReadTransaction srt = removeMessage(null, (StoreId) messageIndex);
                    if (srt != null)
                        srt.commit();
                }
            } catch (MessageLockedException mle) {
                throw mle;
            } catch (Exception e) {
                throw new QueueException(e.getMessage());
            }
        } finally {
            queueLock.unlock();
        }
    }

    public MessageIndex getIndexEntry(MessageIndex messageIndex) throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (!running)
                throw new QueueException("queue " + getQueueName() + " is not running");
            if (queueContent.isEmpty() || !queueContent.contains(messageIndex))
                return null;
            SortedSet tail = ((TreeSet) queueContent).tailSet(messageIndex, true);
            if (tail.isEmpty())
                return null;
            MessageIndex indexEntry = (MessageIndex) tail.first();
            if (indexEntry.equals(messageIndex))
                return indexEntry;
            return null;
        } finally {
            queueLock.unlock();
        }
    }

    public void deleteContent()
            throws QueueException {
        lockAndWaitAsyncFinished();
        try {
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "deleteContent...");
            try {
                if (queueContent != null && queueContent.size() > 0) {
                    queueContent.clear();
                    cache.clear();
                    nStore.close();
                }
                if (pStore != null) {
                    pStore.delete();
                    pStore.close();
                    pStore = null;
                }
            } catch (Exception e) {
                if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "deleteContent, exception=" + e);
                throw new QueueException("exception during delete content: " + e);
            }
            if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "deleteContent done");
        } finally {
            queueLock.unlock();
        }
    }

    public void lockQueue(Object txId) {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "lockQueue ...");
        lockAndWaitAsyncFinished();
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "lockQueue done");
    }

    public void unlockQueue(Object txId, boolean markAsyncActive) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "unlockQueue, markAsyncActive=" + markAsyncActive + " ...");
        asyncActive = markAsyncActive;
        queueLock.unlock();
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "unlockQueue done");
    }

    public void unmarkAsyncActive(Object txId) {
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "unmarkAsyncActive ...");
        queueLock.lock();
        asyncActive = false;
        asyncFinished.signalAll();
        queueLock.unlock();
        if (ctx.queueSpace.enabled) ctx.queueSpace.trace(getQueueName(), "unmarkAsyncActive done");
    }

    public void setCompositeStoreTransaction(Object txId, CompositeStoreTransaction compositeTx) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "setCompositeStoreTransaction compositeTx=" + compositeTx);
        this.compositeTx = compositeTx;
        try {
            if (compositeTx != null)
                compositeTx.setPersistentStore(pStore);
        } catch (StoreException e) {
            e.printStackTrace();
        }
    }

    public CompositeStoreTransaction getCompositeStoreTransaction(Object txId) {
        if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(getQueueName(), "getCompositeStoreTransaction compositeTx=" + compositeTx);
        return compositeTx;
    }

    private class TimeoutProcessor implements Runnable, TimerListener {
        long registrationTime = 0;
        long timeout = 0;
        int id = 0;

        TimeoutProcessor(long registrationTime, long timeout, int id) {
            this.registrationTime = registrationTime;
            this.timeout = timeout;
            this.id = id;
        }

        public void performTimeAction() {
            ctx.threadpoolSwiftlet.runAsync(this);
        }

        public void run() {
            timeoutMessageProcessor(registrationTime, id);
        }
    }

}

