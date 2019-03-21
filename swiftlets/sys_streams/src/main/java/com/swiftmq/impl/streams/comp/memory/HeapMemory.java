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
import com.swiftmq.ms.MessageSelector;
import org.magicwerk.brownies.collections.GapList;

import java.util.*;

/**
 * Memory implementation that stores all Messages on the heap.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class HeapMemory extends Memory {
    MessageStore messageStore = null;

    public HeapMemory(StreamContext ctx) {
        super(ctx, null);
        messageStore = new MessageStore();
    }

    HeapMemory(StreamContext ctx, String name) {
        super(ctx, name);
        messageStore = new MessageStore();
    }

    HeapMemory(StreamContext ctx, List<Entry> list) {
        super(ctx, null);
        this.messageStore = new MessageStore(list);
    }

    @Override
    String getType() {
        return "Heap";
    }

    @Override
    public void reload() throws Exception {

    }

    @Override
    Memory removeByKey(String key) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeByKey, key=" + key + " ...");
        Entry entry = messageStore.remove(key);
        if (entry != null)
            removeFromIndexes(key, entry.message);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeByKey, key=" + key + " done");
        return this;
    }

    @Override
    Message getByKey(String key) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/getByKey, key=" + key + " ...");
        Entry entry = messageStore.get(key);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/getByKey, key=" + key + " done");
        return entry != null ? entry.message : null;
    }

    @Override
    public Memory add(Message message) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add ...");
        if (isLate(message))
            return this;
        long time = getStoreTime(message);
        checkLimit();
        String key = newKey();
        messageStore.add(key, new Entry(key, time, message));
        addToIndexes(key, message);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add done");
        return this;
    }

    @Override
    public Memory remove(int index) throws Exception {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, idx=" + index + " ...");
        Entry entry = messageStore.remove(index);
        if (entry != null)
            removeFromIndexes(entry.key, entry.message);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, idx=" + index + " done");
        return this;
    }

    @Override
    public Memory removeOlderThan(long time) throws Exception {
        return removeOlderThan(time, false);
    }

    @Override
    public Memory removeOlderThan(long time, boolean executeCallback) throws Exception {
        List<Entry> removed = messageStore.removeOlderThan(time);
        for (int i = 0; i < removed.size(); i++) {
            Entry entry = removed.get(i);
            removeFromIndexes(entry.key, entry.message);
        }
        if (orderBy != null && removed.size() > 0)
            lastWindowCloseTime = removed.get(removed.size() - 1).storeTime;
        if (executeCallback && retirementCallback != null && removed.size() > 0)
            retirementCallback.execute(new HeapMemory(ctx, removed));
        return this;
    }

    @Override
    public Message at(int index) throws Exception {
        if (messageStore.size() > index)
            return messageStore.get(index).message;
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/at, idx=" + index + " returns null!");
        return null;
    }

    @Override
    public long getStoreTime(int index) {
        return messageStore.get(index).storeTime;
    }

    @Override
    List<Entry> all() throws Exception {
        return messageStore.all();
    }

    @Override
    public int size() throws Exception {
        return messageStore.size();
    }

    @Override
    public Memory clear() throws Exception {
        messageStore.clear();
        clearIndexes();
        return this;
    }

    @Override
    public void deferredClose() {
        super.deferredClose();
        try {
            clear();
        } catch (Exception e) {

        }
        ctx.stream.removeMemory(this);
    }

    @Override
    public Memory select(String selector) throws Exception {
        MessageSelector sel = new MessageSelector(selector);
        sel.compile();
        Memory child = new HeapMemory(ctx);
        for (int i = 0; i < size(); i++) {
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
        List<Entry> list = all();
        for (Iterator<Entry> iter = list.listIterator(); iter.hasNext(); ) {
            Entry entry = iter.next();
            if (entry.message.isSelected(sel))
                removeByKey(entry.key);
        }
        return this;
    }

    @Override
    Memory create(StreamContext ctx) {
        return new HeapMemory(ctx);
    }

    @Override
    Memory create(StreamContext ctx, List<Entry> list) {
        return new HeapMemory(ctx, list);
    }

    public String toString() {
        return "HeapMemory{" +
                "name='" + name + '\'' +
                '}';
    }

    private class MessageStore implements Comparator<Entry> {
        Map<String, Entry> messages = new TreeMap<String, Entry>();
        GapList<Entry> entryList = new GapList<Entry>();

        public MessageStore() {
        }

        MessageStore(List<Entry> list) {
            for (int i = 0; i < list.size(); i++) {
                Entry entry = list.get(i);
                add(entry.key, entry);
            }
        }

        @Override
        public int compare(Entry o1, Entry o2) {
            return o1.storeTime == o2.storeTime ? 0 : o1.storeTime < o2.storeTime ? -1 : 1;
        }

        private int findIndex(Entry entry) {
            if (entryList.size() == 0)
                return 0;
            int idx = entryList.binarySearch(entry, this);
            if (idx < 0)
                idx = -(idx + 1);
            return idx;
        }

        void add(String key, Entry entry) {
            messages.put(key, entry);
            if (orderBy() == null)
                entryList.add(entry);
            else
                entryList.add(findIndex(entry), entry);
        }

        Entry remove(String key) {
            Entry entry = messages.remove(key);
            entryList.remove(entry);
            return entry;
        }

        Entry remove(int idx) {
            return messages.remove(entryList.remove(idx).key);
        }

        Entry get(String key) {
            return messages.get(key);
        }

        Entry get(int idx) {
            return entryList.get(idx);
        }

        List<Entry> all() {
            return entryList;
        }

        List<Entry> removeOlderThan(long time) {
            List<Entry> removed = new ArrayList<Entry>();
            for (Iterator<Entry> iter = entryList.iterator(); iter.hasNext(); ) {
                Entry entry = iter.next();
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
