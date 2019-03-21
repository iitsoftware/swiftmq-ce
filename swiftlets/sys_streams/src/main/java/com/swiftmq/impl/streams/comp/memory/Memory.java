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
import com.swiftmq.impl.streams.comp.memory.limit.Limit;
import com.swiftmq.impl.streams.comp.memory.limit.LimitBuilder;
import com.swiftmq.impl.streams.comp.message.Message;
import com.swiftmq.impl.streams.comp.message.Property;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;

import java.util.*;

/**
 * Abstract base class for Memories.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public abstract class Memory {
    StreamContext ctx;
    String name;
    List<Limit> limits;
    Map<String, Index> indexes = new HashMap<String, Index>();
    long startTime;
    long inc;
    Entity usage;
    RetirementCallback retirementCallback;
    String orderBy;
    InactivityTimeout inactivityTimeout;
    long lastActivity = Long.MAX_VALUE;
    long lastWindowCloseTime = -1;
    boolean dropLateArrivals = false;
    boolean markedAsClose = false;

    Memory(StreamContext ctx, String name) {
        this.ctx = ctx;
        this.name = name;
        startTime = System.currentTimeMillis();
        inc = 0;
        if (name != null) {
            try {
                EntityList memoryList = (EntityList) ctx.usage.getEntity("memories");
                usage = memoryList.createEntity();
                usage.setName(name);
                usage.createCommands();
                memoryList.addEntity(usage);
                usage.getProperty("memory-type").setValue(getType());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Returns the name of this Memory
     *
     * @return Name
     */
    public String name() {
        return name;
    }

    /**
     * Returns a new LimitBuilder
     *
     * @return LimitBuilder
     */
    public LimitBuilder limit() {
        return new LimitBuilder(ctx, this);
    }

    /**
     * Internal use.
     */
    public Limit addLimit(Limit limit) {
        if (limits == null)
            limits = new ArrayList<Limit>();
        limits.add(limit);
        return limit;
    }

    /**
     * Sets the Property name that is used to order this Memory. This is useful to
     * order a Memory by event time instead of processing time. Default is processing time
     * which is the time at which the Message is added to the Memory.
     *
     * @param orderByProperty Name of the Property
     * @return this
     */
    public Memory orderBy(String orderByProperty) {
        this.orderBy = orderByProperty;
        return this;
    }

    /**
     * Returns the orderBy Property name.
     *
     * @return Property name
     */
    public String orderBy() {
        return orderBy;
    }

    /**
     * Sets and returns the InactivityTimeout for this Memory. An InactivityTimeout can be
     * attached to a Memory and specifies a time of inactivity (no adds to Memory)
     * after which all Messages in that Memory will retire. Inactivity is checked during Memory.checkLimit()
     * and thus needs to be regularly called from a Timer.
     *
     * @return
     */
    public InactivityTimeout inactivityTimeout() {
        if (inactivityTimeout == null)
            inactivityTimeout = new InactivityTimeout(ctx, this);
        return inactivityTimeout;
    }

    /**
     * Specifies whether late arrivals should be dropped (default is false). Late arrivals are only
     * respected if orderBy(propName) is set (thus, event time processing). A late arrival is defined
     * as an event time that is less the time of the maximum event time of the previous window close
     * (onRetire).
     *
     * @param dropLateArrivals true/false
     * @return this
     */
    public Memory dropLateArrivals(boolean dropLateArrivals) {
        this.dropLateArrivals = dropLateArrivals;
        return this;
    }

    protected boolean isLate(Message message) {
        return dropLateArrivals && orderBy != null && message.property(orderBy).value().toLong() < lastWindowCloseTime;
    }

    protected long getStoreTime(Message message) {
        lastActivity = System.currentTimeMillis();
        if (orderBy == null || message.property(orderBy).value().toObject() == null)
            return lastActivity;
        return message.property(orderBy).value().toLong();
    }

    /**
     * Internal use.
     */
    public List<Object> getStoreTimes() throws Exception {
        List<Object> storeTimes = new ArrayList<Object>(size());
        List<Entry> all = all();
        for (int i = 0; i < all.size(); i++) {
            storeTimes.add(all.get(i).storeTime);
        }
        return storeTimes;
    }

    /**
     * Internal use.
     */
    public abstract long getStoreTime(int index);

    /**
     * Creates an Index over a Property
     *
     * @param propName Property Name
     * @return Memory
     */
    public Memory createIndex(String propName) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/createIndex, propName=" + propName);
        Index index = new Index(ctx, this, propName);
        indexes.put(propName, index);
        try {
            List<Entry> list = all();
            for (int i = 0; i < list.size(); i++) {
                Entry entry = list.get(i);
                index.add(entry.message.property(propName).value().toObject(), entry.key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Returns the Index of the specific Property name
     *
     * @param propName Property Name
     * @return Index
     */
    public Index index(String propName) {
        return indexes.get(propName);
    }

    protected String newKey() {
        return new StringBuffer().append(startTime).append('-').append(inc++).toString();
    }

    protected void addToIndexes(String key, Message message) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/addToIndexes, key=" + key + " ...");
        for (Iterator<Map.Entry<String, Index>> iter = indexes.entrySet().iterator(); iter.hasNext(); ) {
            Index index = iter.next().getValue();
            index.add(message.property(index.name()).value().toObject(), key);
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/addToIndexes, key=" + key + " done");
    }

    protected void removeFromIndexes(String key, Message message) {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeFromIndexes, key=" + key + " ...");
        for (Iterator<Map.Entry<String, Index>> iter = indexes.entrySet().iterator(); iter.hasNext(); ) {
            Index index = iter.next().getValue();
            index.remove(message.property(index.name()).value().toObject(), key);
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/removeFromIndexes, key=" + key + " done");
    }

    protected void clearIndexes() {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/clearIndexes ...");
        for (Iterator<Map.Entry<String, Index>> iter = indexes.entrySet().iterator(); iter.hasNext(); ) {
            Index index = iter.next().getValue();
            index.clear();
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/clearIndexes done");
    }

    abstract String getType();

    abstract Memory removeByKey(String key) throws Exception;

    abstract Message getByKey(String key) throws Exception;

    abstract List<Entry> all() throws Exception;

    abstract Memory create(StreamContext ctx);

    abstract Memory create(StreamContext ctx, List<Entry> list);

    /**
     * Internal use.
     */
    public abstract void reload() throws Exception;

    /**
     * Add a Message to the Memory.
     *
     * @param message Message
     * @return Memory
     * @throws Exception
     */
    public abstract Memory add(Message message) throws Exception;

    /**
     * Removes the Message at the index.
     *
     * @param index index
     * @return Memory
     * @throws Exception
     */
    public abstract Memory remove(int index) throws Exception;

    /**
     * Removes all Messages with a store time older than time.
     *
     * @param time Time
     * @return Memory
     * @throws Exception
     */
    public abstract Memory removeOlderThan(long time) throws Exception;

    /**
     * Internal use.
     */
    public abstract Memory removeOlderThan(long time, boolean executeCallback) throws Exception;

    /**
     * Returns the Message at the index.
     *
     * @param index index
     * @return Message
     * @throws Exception
     */
    public abstract Message at(int index) throws Exception;

    /**
     * Returns the number of Messages stored in this Memory.
     *
     * @return Number Messages
     * @throws Exception
     */
    public abstract int size() throws Exception;

    /**
     * Removes all Messages from this Memory.
     *
     * @return Memory
     * @throws Exception
     */
    public abstract Memory clear() throws Exception;

    /**
     * Applies a JMS message selector to all Messages in this Memory and returns a new
     * non-queue Memory with the result.
     *
     * @param selector JMS Message Selector
     * @return Result
     * @throws Exception
     */
    public abstract Memory select(String selector) throws Exception;

    /**
     * Removes all Messages from this Memory that matches the JMS message selector
     * and returns this Memory.
     *
     * @param selector JMS Message Selector
     * @return Memory (this)
     * @throws Exception
     */
    public abstract Memory remove(String selector) throws Exception;

    void forceRetire() {
        try {
            if (size() > 0 && retirementCallback != null)
                retirementCallback.execute(this);
            clear();
        } catch (Exception e) {
            ctx.logStackTrace(e);
        }
    }

    /**
     * Checks the limit of the Memory and retires Messages, if necessary.
     * These retired Messages will be passed to the RetirementListener, if set.
     * The method checks also if there is an inactivity timeout and retires
     * all Messages when a timeout occurs.
     *
     * @return Memory (this)
     */
    public Memory checkLimit() {
        try {
            if (size() == 0)
                return this;
            if (inactivityTimeout != null) {
                if (lastActivity < System.currentTimeMillis() - inactivityTimeout.getMillis()) {
                    if (retirementCallback != null)
                        retirementCallback.execute(this);
                    if (orderBy != null)
                        lastWindowCloseTime = last().property(orderBy).value().toLong();
                    clear();
                    return this;
                }
            }
        } catch (Exception e) {
            ctx.logStackTrace(e);
        }
        if (limits != null) {
            for (int i = 0; i < limits.size(); i++)
                limits.get(i).checkLimit();
        }
        return this;
    }

    /**
     * Returns the first Message of this Memory.
     *
     * @return Message
     * @throws Exception
     */
    public Message first() throws Exception {
        return at(0);
    }

    /**
     * Returns the last Message of this Memory.
     *
     * @return Message
     * @throws Exception
     */
    public Message last() throws Exception {
        return size() == 0 ? null : at(size() - 1);
    }

    /**
     * Reverses the Message order of this Memory and returns a new Memory with the result.
     *
     * @return Result
     * @throws Exception
     */
    public Memory reverse() throws Exception {
        List<Entry> list = all();
        Collections.reverse(list);
        return create(ctx, list);
    }

    /**
     * Returns all values of a Property in the Message order.
     *
     * @param propName Property Name
     * @return List of values
     * @throws Exception
     */
    public List<Object> values(String propName) throws Exception {
        List<Object> list = new ArrayList<Object>(size());
        for (int i = 0; i < size(); i++) {
            Message message = at(i);
            Property property = message.property(propName);
            list.add(property.value().toObject());
        }
        return list;
    }

    /**
     * Groups all Messages over a Property and returns a Group Result.
     *
     * @param propName Property Name
     * @return GroupResult
     * @throws Exception
     */
    public GroupResult group(String propName) throws Exception {
        Map<Object, Memory> map = new HashMap<Object, Memory>();
        for (int i = 0; i < size(); i++) {
            Message message = at(i);
            Property property = message.property(propName);
            Object value = property.value().toObject();
            Memory mem = map.get(value);
            if (mem == null) {
                mem = create(ctx);
                map.put(value, mem);
            }
            mem.add(message);
        }
        return new GroupResult(ctx, propName, map.values().toArray(new Memory[map.size()]));
    }

    /**
     * Sorts all Messages of this Memory over a Property and returns a new Memory with the result.
     *
     * @param propName Property Name
     * @return Result
     * @throws Exception
     */
    public Memory sort(final String propName) throws Exception {
        List<Entry> list = new ArrayList<Entry>(size());
        list.addAll(all());
        Collections.sort(list, new Comparator<Entry>() {
            @Override
            public int compare(Entry o1, Entry o2) {
                Comparable val1 = (Comparable) o1.message.property(propName).value().toObject();
                Comparable val2 = (Comparable) o2.message.property(propName).value().toObject();
                return val1.compareTo(val2);
            }
        });
        return create(ctx, list);
    }

    /**
     * Returns the Message with the minimum value of a Property.
     *
     * @param propName Property Name
     * @return Message
     * @throws Exception
     */
    public Message min(String propName) throws Exception {
        Comparable value = null;
        Message selected = null;
        for (int i = 0; i < size(); i++) {
            Message message = at(i);
            Property property = message.property(propName);
            Comparable val = (Comparable) property.value().toObject();
            if (value == null || val.compareTo(value) < 0) {
                value = val;
                selected = message;
            }
        }
        return selected;
    }

    /**
     * Returns the Message with the maximum value of a Property.
     *
     * @param propName Property Name
     * @return Message
     * @throws Exception
     */
    public Message max(String propName) throws Exception {
        Comparable value = null;
        Message selected = null;
        for (int i = 0; i < size(); i++) {
            Message message = at(i);
            Property property = message.property(propName);
            Comparable val = (Comparable) property.value().toObject();
            if (value == null || val.compareTo(value) > 0) {
                value = val;
                selected = message;
            }
        }
        return selected;
    }

    /**
     * Returns the sum of all values of a Property.
     *
     * @param propName Property Name
     * @return sum
     * @throws Exception
     */
    public double sum(String propName) throws Exception {
        double sum = 0.0;
        for (int i = 0; i < size(); i++)
            sum += at(i).property(propName).value().toDouble();
        return sum;
    }

    /**
     * Returns the average of all values of a Property.
     *
     * @param propName Property Name
     * @return average
     * @throws Exception
     */
    public double average(String propName) throws Exception {
        if (size() == 0)
            return 0.0;
        return sum(propName) / size();
    }

    /**
     * Returns true if all values of a Property are greater than the previous value.
     *
     * @param propName Property Name
     * @return true/false
     * @throws Exception
     */
    public boolean ascendingSeries(String propName) throws Exception {
        Comparable prev = null;
        for (int i = 0; i < size(); i++) {
            Comparable val = (Comparable) at(i).property(propName).value().toObject();
            if (i > 0 && prev.compareTo(val) >= 0)
                return false;
            prev = val;
        }
        return true;
    }

    /**
     * Returns true if all values of a Property are less than the previous value.
     *
     * @param propName Property Name
     * @return true/false
     * @throws Exception
     */
    public boolean descendingSeries(String propName) throws Exception {
        Comparable prev = null;
        for (int i = 0; i < size(); i++) {
            Comparable val = (Comparable) at(i).property(propName).value().toObject();
            if (i > 0 && prev.compareTo(val) <= 0)
                return false;
            prev = val;
        }
        return true;
    }

    /**
     * Performs an inner join with the right Memory over the named join Property name which
     * must exists in the Messages of both Memories.
     * <p/>
     * Result is a Memory which contains Messages where each Message at the left side (this Memory)
     * matches with a Message on the right side. The result Message will contain the left result Message
     * enriched with all Properties of the right result Message.
     *
     * @param right    Memory to join with
     * @param joinProp Name of the Property to join over
     * @return result Memory
     * @throws Exception
     */
    public Memory join(Memory right, String joinProp) throws Exception {
        return join(right, joinProp, joinProp);
    }

    /**
     * Performs an inner join with the right Memory. Use this method to join 2 Memories over different Property names.
     *
     * @param right         Memory to join with
     * @param leftJoinProp  left join Property name
     * @param rightJoinProp right join Property name
     * @return result Memory
     * @throws Exception
     */
    public Memory join(Memory right, String leftJoinProp, String rightJoinProp) throws Exception {
        if (right.index(rightJoinProp) == null) {
            right.createIndex(rightJoinProp);
        }
        Memory result = new HeapMemory(ctx);
        for (int i = 0; i < size(); i++) {
            Message leftMessage = at(i);
            Memory rightResult = right.index(rightJoinProp).get(leftMessage.property(leftJoinProp).value().toObject());
            for (int j = 0; j < rightResult.size(); j++) {
                Message rightMessage = rightResult.at(j);
                Message leftCopy = ctx.messageBuilder.copyMessage(leftMessage);
                leftCopy.copyProperties(rightMessage);
                result.add(leftCopy);
            }
        }
        return result;

    }

    /**
     * Executes the Callback for each Message in this Memory
     *
     * @param callback Callback
     * @throws Exception
     */
    public void forEach(ForEachMessageCallback callback) throws Exception {
        for (int i = 0; i < size(); i++)
            callback.execute(at(i));
    }

    /**
     * Sets a callback which is called for each Message that is retired (removed by a Limit).
     *
     * @param callback Callback
     * @return
     */
    public Memory onRetire(RetirementCallback callback) {
        this.retirementCallback = callback;
        return this;
    }

    /**
     * Returns the RetirementCallback.
     *
     * @return RetirementCallback
     */
    public RetirementCallback retirementCallback() {
        return retirementCallback;
    }

    /**
     * Internal use.
     */
    public void collect(long interval) {
        try {
            usage.getProperty("size").setValue(size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Internal use.
     */
    public void deferredClose() {
        try {
            if (usage != null)
                ctx.usage.getEntity("memories").removeEntity(usage);
        } catch (Exception e) {
        }

    }

    /**
     * Internal use.
     */
    public boolean isMarkedAsClose() {
        return markedAsClose;
    }

    /**
     * Closes this Memory.
     */
    public void close() {
        markedAsClose = true;
    }

    protected class Entry {
        String key;
        long storeTime;
        Message message;

        public Entry(String key, long storeTime, Message message) {
            this.key = key;
            this.storeTime = storeTime;
            this.message = message;
        }
    }
}
