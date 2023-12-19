/*
 * Copyright 2023 IIT Software GmbH
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

package com.swiftmq.impl.threadpool.standard.layer;

import com.swiftmq.tools.collection.ConcurrentList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class Layer {
    private final String identifier;
    private final int priority;
    private final List<EventLoopImpl> eventLoops;

    public Layer(String identifier, int priority) {
        this.identifier = identifier;
        this.priority = priority;
        this.eventLoops = new ConcurrentList<>(new ArrayList<>());
    }

    public String getIdentifier() {
        return identifier;
    }

    public int getPriority() {
        return priority;
    }

    public void addEventLoop(EventLoopImpl eventLoop) {
        eventLoops.add(eventLoop);
        eventLoop.setCloseListener(this::removeEventLoop);
    }

    public void removeEventLoop(EventLoopImpl eventLoop) {
        eventLoops.remove(eventLoop);
    }

    public int size() {
        return eventLoops.size();
    }

    public CompletableFuture<Void> freezeLayer() {
        // Future to indicate completion of freezing all EventLoops in the layer
        CompletableFuture<Void> allFrozen = new CompletableFuture<>();
        List<Future<Void>> freezeFutures = new ArrayList<>();

        for (EventLoopImpl loop : eventLoops) {
            freezeFutures.add(loop.freeze());
        }

        CompletableFuture.allOf(freezeFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> allFrozen.complete(null));

        return allFrozen;
    }

    public void unfreezeLayer() {
        for (EventLoopImpl loop : eventLoops) {
            loop.unfreeze();
        }
    }

    public void close() {
        for (EventLoopImpl loop : eventLoops) {
            loop.close();
        }
    }

    @Override
    public String toString() {
        return "Layer" +
                " identifier=" + identifier +
                ", priority=" + priority;
    }
}
