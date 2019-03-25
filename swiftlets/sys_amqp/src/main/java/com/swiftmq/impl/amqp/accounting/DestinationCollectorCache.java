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

package com.swiftmq.impl.amqp.accounting;

import com.swiftmq.impl.amqp.SwiftletContext;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DestinationCollectorCache {
    SwiftletContext ctx = null;
    private String tracePrefix = null;
    private Map cache = new HashMap();

    public DestinationCollectorCache(SwiftletContext ctx, String tracePrefix) {
        this.ctx = ctx;
        this.tracePrefix = tracePrefix;
    }

    private String createKey(String destinationName, String destinationType, String accountingType) {
        StringBuffer b = new StringBuffer(destinationName);
        b.append('&');
        b.append(destinationType);
        b.append('&');
        b.append(accountingType);
        return b.toString();
    }

    public DestinationCollector getDestinationCollector(String destinationName, String destinationType, String accountingType) {
        String key = createKey(destinationName, destinationType, accountingType);
        DestinationCollector collector = (DestinationCollector) cache.get(key);
        if (collector == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.getDestinationCollector, key=" + key + ", not found, creating new one");
            collector = new DestinationCollector(key, destinationName, destinationType, accountingType);
            cache.put(key, collector);
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.getDestinationCollector, found, collector=" + collector);
        }
        return collector;
    }

    public void flush(AMQPSource source, String userName, String clientId, String remoteHostName, String smqpVersion) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.flush ...");
        String timestamp = DestinationCollector.fmt.format(new Date());
        for (Iterator iter = cache.entrySet().iterator(); iter.hasNext(); ) {
            DestinationCollector collector = (DestinationCollector) ((Map.Entry) iter.next()).getValue();
            if (collector.isDirty()) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.flush, collector=" + collector);
                source.send(timestamp, userName, clientId, remoteHostName, smqpVersion, collector);
                collector.clear();
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.flush done");
    }

    public void add(DestinationCollector collector) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.add, collector=" + collector);
        cache.put(collector.getKey(), collector);
    }

    public void remove(DestinationCollector collector) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$amqp", tracePrefix + "/DestinationCollectorCache.remove, collector=" + collector);
        cache.remove(collector.getKey());
    }

    public void clear() {
        cache.clear();
    }
}
