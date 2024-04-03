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

package com.swiftmq.impl.amqp.amqp.v00_09_01;

import com.swiftmq.impl.amqp.SwiftletContext;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueMapper {
    SwiftletContext ctx = null;
    Map<String, String> mapping = new ConcurrentHashMap<>();

    public QueueMapper(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    public void map(String name, String mapTo) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/map " + name + " to " + mapTo);
        mapping.put(name, mapTo);
    }

    public void unmap(String name) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/unmap " + name);
        mapping.remove(name);
    }

    public String get(String name) {
        return mapping.get(name);
    }

    public void unmapTempQueue(String mapTo) {
        for (Iterator<Map.Entry<String, String>> iter = mapping.entrySet().iterator(); iter.hasNext(); ) {
            String s = iter.next().getValue();
            if (s.equals(mapTo)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/unmapTempQueue " + mapTo);
                iter.remove();
                break;
            }
        }
    }

    public String toString() {
        return "QueueMapper";
    }
}
