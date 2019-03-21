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
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.*;
import org.magicwerk.brownies.collections.GapList;

import javax.jms.DeliveryMode;
import java.util.*;

/**
 * Memory implementation that stores all Messages as persistent Messages in a regular Queue.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class QueueMemory extends Memory {
    static final String KEY = "_memory";
    String queueName;
    AbstractQueue abstractQueue;
    QueueSender sender;
    TxBookKeeper txBookKeeper;
    MessageStore messageStore;
    boolean shared = false;

    QueueMemory(StreamContext ctx, String name, String queueName) throws Exception {
        super(ctx, name);
        this.queueName = queueName;
        abstractQueue = ctx.ctx.queueManager.getQueueForInternalUse(queueName);
        sender = ctx.ctx.queueManager.createQueueSender(queueName, null);
        messageStore = new MessageStore();
        txBookKeeper = new TxBookKeeper(ctx, queueName);
    }


    private List getContent() throws QueueException {
        return new ArrayList(abstractQueue.getQueueIndex());
    }

    protected String getQueueName() {
        return queueName;
    }

    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }

    @Override
    String getType() {
        return "Queue";
    }

    /**
     * Marks the Queue that is used as backstore as shared by multiple QueueMemories.
     * This QueueMemory will then add a Property "_memory" to each Message to identify
     * the Messages of this QueueMemory.
     *
     * @return this
     */
    public QueueMemory shared() {
        this.shared = true;
        return this;
    }

    @Override
    public void reload() throws Exception {
        List content = getContent();
        for (int i = 0; i < content.size(); i++) {
            MessageIndex messageIndex = (MessageIndex) content.get(i);
            MessageImpl message = abstractQueue.getMessageByIndex(messageIndex).getMessage();
            if (!shared || message.propertyExists(KEY) && message.getStringProperty(KEY).equals(name)) {
                String key = message.getJMSMessageID();
                long storeTime = message.getJMSTimestamp();
                messageStore.add(key, new KeyEntry(key, messageIndex, storeTime));
                addToIndexes(key, ctx.messageBuilder.wrap(message));
            }
        }
    }

    @Override
    Memory removeByKey(String key) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeByKey, key=" + key + " ...");
        removeFromIndexes(key, getByKey(key));
        KeyEntry keyEntry = messageStore.remove(key);
        txBookKeeper.remove(keyEntry.key, keyEntry);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeByKey, key=" + key + " done");
        return this;
    }

    @Override
    Message getByKey(String key) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/getByKey, key=" + key + " ...");
        KeyEntry keyEntry = messageStore.get(key);
        if (keyEntry != null && keyEntry.messageIndex != null)
            return ctx.messageBuilder.wrap(abstractQueue.getMessageByIndex(keyEntry.messageIndex).getMessage());
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/getByKey, key=" + key + " done");
        return txBookKeeper.get(key);
    }

    @Override
    public Memory add(Message message) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add ...");
        if (isLate(message))
            return this;
        String key = message.messageId();
        if (key == null)
            key = ctx.nextId();
        if (messageStore.get(key) != null) {
            // Duplicate!
            return this;
        }
        long storeTime = getStoreTime(message);
        checkLimit();
        KeyEntry keyEntry = new KeyEntry(key, null, storeTime);
        messageStore.add(key, keyEntry);
        Message copy = ctx.messageBuilder.copyMessage(message);
        MessageImpl impl = copy.getImpl();
        if (impl.getJMSDestination() == null)
            impl.setJMSDestination(new QueueImpl(queueName));
        impl.setJMSPriority(0); // Important for the ordering
        impl.setJMSDeliveryMode(getDeliveryMode());
        impl.setJMSTimestamp(storeTime);
        if (key.startsWith("ID:"))
            impl.setJMSMessageID(key.substring(3));
        else
            impl.setJMSMessageID(key);
        if (shared)
            impl.setStringProperty(KEY, name);
        txBookKeeper.add(key, copy, keyEntry);
        addToIndexes(key, copy);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add done");
        return this;
    }

    @Override
    public Memory remove(int index) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, idx=" + index + " ...");
        KeyEntry keyEntry = messageStore.remove(index);
        txBookKeeper.remove(keyEntry.key, keyEntry);
        removeFromIndexes(keyEntry.key, getByKey(keyEntry.key));
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, idx=" + index + " done");
        return this;
    }

    @Override
    public Memory removeOlderThan(long time) throws Exception {
        return removeOlderThan(time, false);
    }

    @Override
    public long getStoreTime(int index) {
        return messageStore.get(index).storeTime;
    }

    @Override
    public Memory removeOlderThan(long time, boolean executeCallback) throws Exception {
        Memory retired = null;
        List<KeyEntry> removed = messageStore.removeOlderThan(time);
        if (orderBy != null && removed.size() > 0)
            lastWindowCloseTime = removed.get(removed.size() - 1).storeTime;
        if (executeCallback && retirementCallback != null && removed.size() > 0)
            retired = new HeapMemory(ctx);
        for (int i = 0; i < removed.size(); i++) {
            KeyEntry keyEntry = removed.get(i);
            if (keyEntry.messageIndex != null) {
                MessageEntry entry = abstractQueue.getMessageByIndex(keyEntry.messageIndex);
                txBookKeeper.remove(keyEntry.key, keyEntry);
                Message message = ctx.messageBuilder.wrap(entry.getMessage());
                removeFromIndexes(keyEntry.key, message);
                if (retired != null)
                    retired.add(message);
            }
        }
        if (retired != null && retired.size() > 0)
            retirementCallback.execute(retired);
        return this;
    }

    @Override
    public Message at(int index) throws Exception {
        KeyEntry keyEntry = messageStore.get(index);
        if (keyEntry.messageIndex != null)
            return ctx.messageBuilder.wrap(abstractQueue.getMessageByIndex(keyEntry.messageIndex).getMessage());
        return txBookKeeper.get(keyEntry.key);
    }

    @Override
    List<Entry> all() throws Exception {
        List<Entry> result = new ArrayList<Entry>();
        for (int i = 0; i < messageStore.size(); i++) {
            KeyEntry keyEntry = messageStore.get(i);
            if (keyEntry.messageIndex != null) {
                MessageEntry entry = abstractQueue.getMessageByIndex(keyEntry.messageIndex);
                result.add(new Entry(keyEntry.key, entry.getMessage().getJMSTimestamp(), ctx.messageBuilder.wrap(entry.getMessage())));
            } else {
                Message message = txBookKeeper.get(keyEntry.key);
                result.add(new Entry(keyEntry.key, message.timestamp(), message));
            }
        }
        return result;
    }

    @Override
    public int size() throws Exception {
        return messageStore.size();
    }

    @Override
    public Memory clear() throws Exception {
        List<KeyEntry> list = messageStore.all();
        for (int i = 0; i < list.size(); i++) {
            KeyEntry keyEntry = list.get(i);
            if (keyEntry.messageIndex != null) {
                MessageEntry entry = abstractQueue.getMessageByIndex(keyEntry.messageIndex);
                messageStore.remove(keyEntry.key);
                txBookKeeper.remove(keyEntry.key, keyEntry);
                removeFromIndexes(keyEntry.key, ctx.messageBuilder.wrap(entry.getMessage()));
            }
        }
        return this;
    }

    @Override
    public Memory select(String selector) throws Exception {
        MessageSelector sel = new MessageSelector(selector);
        sel.compile();
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < messageStore.size(); i++) {
            Message message = at(i);
            if (message.isSelected(sel))
                child.add(message);
        }
        return child;
    }

    @Override
    public Memory remove(String selector) throws Exception {
        MessageSelector sel = new MessageSelector(selector);
        sel.compile();
        List<KeyEntry> list = messageStore.all();
        for (int i = 0; i < list.size(); i++) {
            KeyEntry keyEntry = list.get(i);
            if (keyEntry.messageIndex != null) {
                MessageEntry entry = abstractQueue.getMessageByIndex(keyEntry.messageIndex);
                if (sel.isSelected(entry.getMessage())) {
                    messageStore.remove(keyEntry.key);
                    txBookKeeper.remove(keyEntry.key, keyEntry);
                    removeFromIndexes(keyEntry.key, ctx.messageBuilder.wrap(entry.getMessage()));
                }
            }
        }
        txBookKeeper.remove(sel);
        return this;
    }

    @Override
    public void deferredClose() {
        super.deferredClose();
        txBookKeeper.close();
        try {
            sender.close();
        } catch (Exception e) {

    }
        ctx.stream.removeMemory(this);
    }

    @Override
    Memory create(StreamContext ctx) {
        return new HeapMemory(ctx);
    }

    @Override
    Memory create(StreamContext ctx, List<Entry> list) {
        return new HeapMemory(ctx, list);
    }

    @Override
    public String toString() {
        return "QueueMemory{" +
                "name='" + name + '\'' +
                "queueName='" + queueName + '\'' +
                '}';
    }

    class KeyEntry {
        String key;
        MessageIndex messageIndex;
        long storeTime;

        public KeyEntry(String key, MessageIndex messageIndex, long storeTime) {
            this.key = key;
            this.messageIndex = messageIndex;
            this.storeTime = storeTime;
        }

        @Override
        public String toString() {
            return "KeyEntry{" +
                    "key='" + key + '\'' +
                    ", messageIndex=" + messageIndex +
                    ", storeTime=" + storeTime +
                    '}';
        }
    }

    class MessageStore implements Comparator<KeyEntry> {
        Map<String, KeyEntry> messages = new TreeMap<String, KeyEntry>();
        GapList<KeyEntry> entryList = new GapList<KeyEntry>();

        public MessageStore() {
        }

        MessageStore(List<KeyEntry> list) {
            for (int i = 0; i < list.size(); i++) {
                KeyEntry entry = list.get(i);
                add(entry.key, entry);
            }
        }

        @Override
        public int compare(KeyEntry o1, KeyEntry o2) {
            return o1.storeTime == o2.storeTime ? 0 : o1.storeTime < o2.storeTime ? -1 : 1;
        }

        private int findIndex(KeyEntry entry) {
            if (entryList.size() == 0)
                return 0;
            int idx = entryList.binarySearch(entry, this);
            if (idx < 0)
                idx = -(idx + 1);
            return idx;
        }

        void add(String key, KeyEntry entry) {
            messages.put(key, entry);
            if (orderBy() == null)
                entryList.add(entry);
            else
                entryList.add(findIndex(entry), entry);
        }

        KeyEntry remove(String key) {
            KeyEntry entry = messages.remove(key);
            entryList.remove(entry);
            return entry;
        }

        KeyEntry remove(int idx) {

            return remove(entryList.get(idx).key);
        }

        KeyEntry get(String key) {
            return messages.get(key);
        }

        KeyEntry get(int idx) {
            return entryList.get(idx);
        }

        List<KeyEntry> all() {
            return new ArrayList<KeyEntry>(messages.values());
        }

        List<KeyEntry> removeOlderThan(long time) {
            List<KeyEntry> removed = new ArrayList<KeyEntry>();
            for (Iterator<KeyEntry> iter = entryList.iterator(); iter.hasNext(); ) {
                KeyEntry entry = iter.next();
                if (entry.storeTime < time) {
                    removed.add(entry);
                    messages.remove(entry.key);
                    iter.remove();
                } else
                    break;
            }
            return removed;
        }

        int size() {
            return entryList.size();
        }

        void clear() {
            messages.clear();
            entryList.clear();
        }
    }
}
