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

import java.util.*;

/**
 * Property Index that helds Property Values and References to Messages.
 *
 * @author IIT Software GmbH, Muenster/Germany, (c) 2016, All Rights Reserved
 */
public class Index {
    StreamContext ctx;
    Memory memory;
    String name;
    SortedMap<Object, Entry> indexMap;

    Index(StreamContext ctx, Memory memory, String name) {
        this.ctx = ctx;
        this.memory = memory;
        this.name = name;
        indexMap = new TreeMap<Object, Entry>();
    }

    private Object normalizeValue(Object value) {
        if (value instanceof Double)
            value = ((Double) value).longValue();
        else if (value instanceof Integer)
            value = ((Integer) value).longValue();
        return value;
    }

    Index add(Object value, String key) {
        value = normalizeValue(value);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add, value=" + value + ", key=" + key + " ...");
        Entry entry = indexMap.get(value);
        if (entry == null) {
            if (ctx.ctx.traceSpace.enabled)
                ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add, value=" + value + ", key=" + key + " create new Entry");
            entry = new Entry();
            indexMap.put(value, entry);
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add, value=" + value + ", key=" + key + " done, before add, keys=" + entry.keys);
        entry.keys.add(key);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/add, value=" + value + ", key=" + key + " done, after add, keys=" + entry.keys);
        return this;
    }

    Index remove(Object value, String key) {
        value = normalizeValue(value);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + ", key=" + key + " ...");
        Entry entry = indexMap.get(value);
        if (entry != null) {
            entry.keys.remove(key);
            if (ctx.ctx.traceSpace.enabled)
                ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + ", key=" + key + ", keys=" + entry.keys);
            if (entry.keys.size() == 0) {
                if (ctx.ctx.traceSpace.enabled)
                    ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + ", key=" + key + ", removing indexMap entry");
                indexMap.remove(value);
            }
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + ", key=" + key + " done");
        return this;
    }

    Index clear() {
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/clear");
        indexMap.clear();
        return this;
    }

    /**
     * Returns the name of this Index (= Property Name)
     *
     * @return Name
     */
    public String name() {
        return name;
    }

    /**
     * Selects all Messages with a specific Property Value from the associated Memory and returns the result as new non-queue Memory.
     *
     * @param value Property Value
     * @return Result
     * @throws Exception
     */
    public Memory get(Object value) throws Exception {
        value = normalizeValue(value);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/get, value=" + value + " ...");
        Memory child = new HeapMemory(ctx);
        Entry entry = indexMap.get(value);
        if (entry != null) {
            if (ctx.ctx.traceSpace.enabled)
                ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/get, value=" + value + ", entry.keys=" + entry.keys);
            for (Iterator<String> iter = entry.keys.iterator(); iter.hasNext(); ) {
                String key = iter.next();
                if (ctx.ctx.traceSpace.enabled)
                    ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/get, value=" + value + ", add key=" + key + " to child");
                child.add(memory.getByKey(key));
            }
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/get, value=" + value + ", child.size=" + child.size() + " done");
        return child;
    }

    /**
     * Removes all Messages with a specific Property Value from the associated Memory.
     *
     * @param value Property Value
     * @return Index (this)
     * @throws Exception
     */
    public Index remove(Object value) throws Exception {
        value = normalizeValue(value);
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + " ...");
        Entry entry = indexMap.remove(value);
        if (entry != null) {
            if (ctx.ctx.traceSpace.enabled)
                ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + ", entry.keys=" + entry.keys);
            String[] copy = entry.keys.toArray(new String[entry.keys.size()]);
            for (int i = 0; i < copy.length; i++) {
                memory.removeByKey(copy[i]);
            }
        }
        if (ctx.ctx.traceSpace.enabled)
            ctx.ctx.traceSpace.trace(ctx.ctx.streamsSwiftlet.getName(), toString() + "/remove, value=" + value + " done");
        return this;
    }

    @Override
    public String toString() {
        return "Index{" +
                "name='" + memory.name() + "/" + name + '\'' +
                '}';
    }

    private class Entry {
        Set<String> keys = new LinkedHashSet<String>();

        @Override
        public String toString() {
            return "Entry{" +
                    "keys=" + keys +
                    '}';
        }
    }
}
