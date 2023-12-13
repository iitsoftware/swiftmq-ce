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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ExchangeRegistry {
    SwiftletContext ctx = null;
    Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    Exchange defaultDirect = null;

    public ExchangeRegistry(SwiftletContext ctx) {
        this.ctx = ctx;
        defaultDirect = new Exchange() {
            public int getType() {
                return DIRECT;
            }
        };
        exchanges.put("", defaultDirect);
        exchanges.put("amq.direct", defaultDirect);
    }

    public Exchange get(String name) {
        return exchanges.get(name);
    }

    public Exchange declare(String name, ExchangeFactory exchangeFactory) throws Exception {
        if (exchangeFactory == null) {
            throw new IllegalArgumentException("Exchange factory must not be null");
        }

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        Exchange exchange = exchanges.computeIfAbsent(name, key -> {
            try {
                if (ctx.traceSpace.enabled) {
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/create exchange: " + key);
                }
                return exchangeFactory.create();
            } catch (Exception e) {
                exceptionRef.set(e);
                return null;
            }
        });

        Exception exception = exceptionRef.get();
        if (exception != null) {
            throw exception; // Rethrow the exception caught during exchange creation
        }

        return exchange;
    }

    public void delete(String name, boolean ifUnused) throws Exception {
        exchanges.computeIfPresent(name, (key, exchange) -> {
            if (exchange != defaultDirect) {
                if (ctx.traceSpace.enabled) {
                    ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), this + "/delete exchange: " + key);
                }
                return null; // Return null to indicate removal
            }
            return exchange; // Return exchange to leave it in the map
        });
    }

    public String toString() {
        return "ExchangeRegistry";
    }
}
