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

package com.swiftmq.impl.queue.standard;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class QueueMetrics {
    private final AtomicInteger consumed = new AtomicInteger(0);
    private final AtomicInteger produced = new AtomicInteger(0);
    private final AtomicInteger totalConsumed = new AtomicInteger(0);
    private final AtomicInteger totalProduced = new AtomicInteger(0);
    private final AtomicLong lastConsumedTimestamp = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastProducedTimestamp = new AtomicLong(System.currentTimeMillis());

    public int getConsumingRate() {
        long currentTime = System.currentTimeMillis();
        long lastTimestamp = lastConsumedTimestamp.getAndSet(currentTime);
        int consumedCount = consumed.getAndSet(0);
        double secs = (currentTime - lastTimestamp) / 1000.0;
        return (int) Math.round(consumedCount / secs);
    }

    public int getProducingRate() {
        long currentTime = System.currentTimeMillis();
        long lastTimestamp = lastProducedTimestamp.getAndSet(currentTime);
        int producedCount = produced.getAndSet(0);
        double secs = (currentTime - lastTimestamp) / 1000.0;
        return (int) Math.round(producedCount / secs);
    }

    public int getConsumedTotal() {
        return totalConsumed.get();
    }

    public int getProducedTotal() {
        return totalProduced.get();
    }

    public void resetCounters() {
        totalConsumed.set(0);
        totalProduced.set(0);
    }

    public void incrementConsumed() {
        consumed.incrementAndGet();
        totalConsumed.incrementAndGet();
    }

    public void incrementProduced() {
        produced.incrementAndGet();
        totalProduced.incrementAndGet();
    }
}
