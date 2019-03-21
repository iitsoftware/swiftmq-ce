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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A MemoryGroup groups incoming Messages on base of the values of a Message Property (Group Property).
 * Scripts register a MemoryCreateCallback that is called for each new value of this Property so that
 * a Memory exists for each distinct Property value. The MemoryCreateCallback may also chose to not
 * create a Memory and may return null. In that case the value will not be respected.
 * <p/>
 * A MemoryGroup may be used to implement session windows.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2017, All Rights Reserved
 */
public class MemoryGroup {
    StreamContext ctx;
    String name;
    String groupPropName;
    MemoryCreateCallback memoryCreateCallback;
    MemoryRemoveCallback memoryRemoveCallback;
    Map<Comparable, Entry> memories = new HashMap<Comparable, Entry>();
    GroupInactivityTimeout inactivityTimeout;
    Memory stateMemory = null;
    boolean markedAsClose = false;

    /**
     * Internal use only.
     */
    public MemoryGroup(StreamContext ctx, String name, String groupPropName) {
        this.ctx = ctx;
        this.name = name;
        this.groupPropName = groupPropName;
        this.stateMemory = ctx.stream.stateMemory();
    }

    private void insertStateMemory(Comparable key) {
        try {
            stateMemory.add(ctx.stream.create().message().message().property("memorygroup").set(name).property("key").set(key));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void removeStateMemory(Comparable key) {
        try {
            for (int i = 0; i < stateMemory.size(); i++) {
                Message msg = stateMemory.at(i);
                if (msg.property("memorygroup").value().toObject().equals(name) && msg.property("key").value().toObject().equals(key)) {
                    stateMemory.remove(i);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Object> getAllKeys() {
        try {
            return stateMemory.select("memorygroup = '" + name + "'").values("key");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Returns the name of this MemoryGroup
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the name of the group Property.
     *
     * @return group Property name
     */
    public String groupPropertyName() {
        return groupPropName;
    }

    /**
     * Sets and returns the GroupInactivityTimeout for this Memory. A GroupInactivityTimeout can be
     * attached to a MemoryGroup and specifies a time of inactivity (no adds to attached Memory)
     * after which all Messages in that Memory will retire and the Memory will be closed and
     * removed from the MemoryGroup. Inactivity is checked during MemoryGroup.checkLimit()
     * and thus needs to be regularly called from a Timer.
     *
     * @return GroupInactivityTimeout
     */
    public GroupInactivityTimeout inactivityTimeout() {
        if (inactivityTimeout == null)
            inactivityTimeout = new GroupInactivityTimeout(ctx, this);
        return inactivityTimeout;
    }

    /**
     * Adds a Message to this MemoryGroup. If the value of the group Property is detected
     * for the first time, MemoryCreateCallback.create is called.
     *
     * @param message Message
     * @return this
     * @throws Exception
     */
    public MemoryGroup add(Message message) throws Exception {
        long time = System.currentTimeMillis();
        Comparable key = (Comparable) message.property(groupPropName).value().toObject();
        if (key == null)
            throw new Exception("Group Property '" + groupPropName + "' does not exist in Message");
        Entry entry = memories.get(key);
        if (entry == null) {
            Memory memory = memoryCreateCallback.create(key);
            if (memory != null) {
                entry = new Entry(time, memory);
                memories.put(key, entry);
                insertStateMemory(key);
            }
        }
        if (entry != null) {
            entry.memory.add(message);
            entry.lastActivity = time;
        }
        return this;
    }

    /**
     * Registers a MemoryCreateCallback (mandatory)
     *
     * @param callback MemoryCreateCallback
     * @return this
     */
    public MemoryGroup onCreate(MemoryCreateCallback callback) {
        this.memoryCreateCallback = callback;
        List<Object> keys = getAllKeys();
        long time = System.currentTimeMillis();
        try {
            for (int i = 0; i < keys.size(); i++) {
                Comparable key = (Comparable) keys.get(i);
                Memory memory = memoryCreateCallback.create(key);
                if (memory != null) {
                    Entry entry = new Entry(time, memory);
                    memories.put(key, entry);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Registers a MemoryRemoveCallback (optional)
     *
     * @param callback MemoryRemoveCallback
     * @return this
     */
    public MemoryGroup onRemove(MemoryRemoveCallback callback) {
        this.memoryRemoveCallback = callback;
        return this;
    }

    /**
     * Removes a Memory from this MemoryGroup.
     *
     * @param key Value of the group Property
     * @return this
     */
    public MemoryGroup removeMemory(Comparable key) {
        Entry entry = memories.remove(key);
        if (entry != null)
            removeStateMemory(entry.memory.name());
        return this;
    }

    /**
     * Checks inactivity and calls checkLimit() on each Memory of this MemoryGroup
     *
     * @return this
     */
    public MemoryGroup checkLimit() {
        long time = System.currentTimeMillis();
        for (Iterator<Map.Entry<Comparable, Entry>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            boolean removed = false;
            Map.Entry<Comparable, Entry> mapEntry = iter.next();
            Comparable key = mapEntry.getKey();
            Entry entry = mapEntry.getValue();
            if (inactivityTimeout != null) {
                if (entry.lastActivity < time - inactivityTimeout.millis) {
                    entry.memory.forceRetire();
                    entry.memory.close();
                    if (memoryRemoveCallback != null)
                        memoryRemoveCallback.onRemove(key);
                    removeStateMemory(key);
                    iter.remove();
                    removed = true;
                }
            }
            if (!removed)
                entry.memory.checkLimit();
        }
        return this;
    }

    /**
     * Executes the Callback for each Memory in this MemoryGroup
     *
     * @param callback Callback
     * @throws Exception
     */
    public void forEach(ForEachMemoryCallback callback) {
        for (Iterator<Map.Entry<Comparable, Entry>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<Comparable, Entry> mapEntry = iter.next();
            Entry entry = mapEntry.getValue();
            callback.execute(entry.memory);
        }
    }

    /**
     * Internal Use.
     */
    public void deferredClose() {
        for (Iterator<Map.Entry<Comparable, Entry>> iter = memories.entrySet().iterator(); iter.hasNext(); ) {
            iter.next().getValue().memory.deferredClose();
        }
        memories.clear();
        ctx.stream.removeMemoryGroup(this);
    }

    /**
     * Internal Use.
     */
    public boolean isMarkedAsClose() {
        return markedAsClose;
    }

    /**
     * Closes this MemoryGroup. It closes also all Memories attached to this MemoryGroup
     */
    public void close() {
        markedAsClose = true;
    }

    @Override
    public String toString() {
        return "MemoryGroup {" +
                "name='" + name + '\'' +
                ", groupPropName='" + groupPropName + '\'' +
                '}';
    }

    private class Entry {
        long lastActivity;
        Memory memory;

        public Entry(long lastActivity, Memory memory) {
            this.lastActivity = lastActivity;
            this.memory = memory;
        }
    }
}
