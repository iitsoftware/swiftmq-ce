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

package com.swiftmq.impl.routing.single.accounting;

import com.swiftmq.swiftlet.trace.TraceSpace;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DestinationCollectorCache {
    private TraceSpace traceSpace = null;
    private String tracePrefix = null;
    private Map cache = new HashMap();

    public DestinationCollectorCache(TraceSpace traceSpace, String tracePrefix) {
        this.traceSpace = traceSpace;
        this.tracePrefix = tracePrefix;
    }

    private String createKey(String sourceRouter, String destinationRouter, String destinationName, String destinationType, String directionType) {
        StringBuffer b = new StringBuffer(sourceRouter);
        b.append('&');
        b.append(destinationRouter);
        b.append('&');
        b.append(destinationName);
        b.append('&');
        b.append(destinationType);
        b.append('&');
        b.append(directionType);
        return b.toString();
    }

    public DestinationCollector getDestinationCollector(String sourceRouter, String destinationRouter, String destinationName, String destinationType, String directionType) {
        String key = createKey(sourceRouter, destinationRouter, destinationName, destinationType, directionType);
        DestinationCollector collector = (DestinationCollector) cache.get(key);
        if (collector == null) {
            if (traceSpace.enabled)
                traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.getDestinationCollector, key=" + key + ", not found, creating new one");
            collector = new DestinationCollector(key, sourceRouter, destinationRouter, destinationName, destinationType, directionType);
            cache.put(key, collector);
        } else {
            if (traceSpace.enabled)
                traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.getDestinationCollector, found, collector=" + collector);
        }
        return collector;
    }

    public void flush(RoutingSource source, String localRouter, String connectedRouter, String connectedHostName, String smqprVersion) {
        if (traceSpace.enabled) traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.flush ...");
        String timestamp = DestinationCollector.fmt.format(new Date());
        for (Iterator iter = cache.entrySet().iterator(); iter.hasNext(); ) {
            DestinationCollector collector = (DestinationCollector) ((Map.Entry) iter.next()).getValue();
            if (collector.isDirty()) {
                if (traceSpace.enabled)
                    traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.flush, collector=" + collector);
                source.send(timestamp, localRouter, connectedRouter, connectedHostName, smqprVersion, collector);
                collector.clear();
            }
        }
        if (traceSpace.enabled) traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.flush done");
    }

    public void add(DestinationCollector collector) {
        if (traceSpace.enabled)
            traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.add, collector=" + collector);
        cache.put(collector.getKey(), collector);
    }

    public void remove(DestinationCollector collector) {
        if (traceSpace.enabled)
            traceSpace.trace("sys$routing", tracePrefix + "/DestinationCollectorCache.remove, collector=" + collector);
        cache.remove(collector.getKey());
    }

    public void clear() {
        cache.clear();
    }
}
