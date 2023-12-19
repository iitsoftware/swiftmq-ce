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

import com.swiftmq.impl.threadpool.standard.layer.pool.PlatformThreadRunner;
import com.swiftmq.impl.threadpool.standard.layer.pool.ThreadRunner;
import com.swiftmq.impl.threadpool.standard.layer.pool.VirtualThreadRunner;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.trace.TraceSpace;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class LayerRegistry {
    private static final String DEFAULT_LAYER_ID = "default";
    private static final int DEFAULT_LAYER_PRIORITY = 0;
    private final TraceSpace traceSpace;
    private final String tracePrefix;
    private final Map<String, Layer> layers;
    private final EntityList layerList;
    private EntityListEventAdapter layerAdapter = null;
    private final ThreadRunner platformThreadRunner = new PlatformThreadRunner();
    private final ThreadRunner virtualThreadRunner = new VirtualThreadRunner();
    private final Map<String, LoopData> eventLoopConfig = new ConcurrentHashMap<>();

    public LayerRegistry(EntityList layerList, TraceSpace traceSpace, String tracePrefix) {
        this.layerList = layerList;
        this.traceSpace = traceSpace;
        this.tracePrefix = tracePrefix + "/" + this;
        this.layers = new ConcurrentHashMap<>();
        registerLayer(new Layer(DEFAULT_LAYER_ID, DEFAULT_LAYER_PRIORITY));
        createLayers();
    }

    private void createLayers() {
        layerAdapter = new EntityListEventAdapter(layerList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    final String layerName = newEntity.getName();
                    registerLayer(new Layer(layerName, (Integer) newEntity.getProperty("priority").getValue()));
                    EntityListEventAdapter loopAdapter = new EntityListEventAdapter((EntityList) newEntity.getEntity("eventloops"), true, true) {
                        @Override
                        public void onEntityAdd(Entity parent, Entity newEntity) {
                            eventLoopConfig.put(newEntity.getName(), new LoopData((Boolean) newEntity.getProperty("virtual").getValue(), layerName));
                        }

                        @Override
                        public void onEntityRemove(Entity parent, Entity delEntity) {
                            eventLoopConfig.remove(delEntity.getName());
                        }
                    };
                    loopAdapter.init();
                    newEntity.setUserObject(loopAdapter);
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                deregisterLayer(delEntity.getName());
                EntityListEventAdapter loopAdapter = (EntityListEventAdapter) delEntity.getUserObject();
                if (loopAdapter != null) {
                    try {
                        loopAdapter.close();
                    } catch (Exception e) {
                        throw new EntityRemoveException(e.toString());
                    }
                    delEntity.setUserObject(null);
                }
            }
        };
        try {
            layerAdapter.init();
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    public ThreadRunner threadRunnerForEventLoop(String id) {
        ThreadRunner runner = platformThreadRunner;
        boolean virtual = eventLoopConfig.get(id).virtual();
        if (virtual)
            runner = virtualThreadRunner;
        return runner;
    }

    public void registerLayer(Layer layer) {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "registerLayer, layer=" + layer.getIdentifier());
        layers.put(layer.getIdentifier(), layer);
    }

    public void deregisterLayer(String identifier) {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "deregisterLayer, identifier=" + identifier);
        Layer layer = layers.remove(identifier);
        if (layer != null)
            layer.close();
    }

    public Layer getLayer(String loopName) {
        String layerName;
        if (eventLoopConfig.get(loopName) != null)
            layerName = eventLoopConfig.get(loopName).layer();
        else {
            layerName = DEFAULT_LAYER_ID;
        }
        if (traceSpace.enabled)
            traceSpace.trace(tracePrefix, "getLayer, loopName=" + loopName + " returns=" + layerName);
        return layers.get(layerName);
    }

    public int platformThreads() {
        return platformThreadRunner.getActiveThreadCount();
    }

    public int virtualThreads() {
        return virtualThreadRunner.getActiveThreadCount();
    }

    public int size() {
        int totalSize = 0;
        for (Layer layer : layers.values()) {
            totalSize += layer.size();
        }
        return totalSize;
    }

    public CompletableFuture<Void> freezeLayers() {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "freezeLayers");
        // Sort layers by priority
        var sortedLayers = layers.values().stream()
                .sorted(Comparator.comparingInt(Layer::getPriority))
                .toList();

        CompletableFuture<Void>[] futures = new CompletableFuture[sortedLayers.size()];
        int index = 0;

        for (Layer layer : sortedLayers) {
            CompletableFuture<Void> layerFreezeFuture = new CompletableFuture<>();
            futures[index++] = layerFreezeFuture;

            layer.freezeLayer().thenRun(() -> layerFreezeFuture.complete(null));
        }

        return CompletableFuture.allOf(futures);
    }

    public void unfreezeLayers() {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "unfreezeLayers");
        // Unfreeze layers in reverse priority order
        var sortedLayers = layers.values().stream()
                .sorted(Comparator.comparingInt(Layer::getPriority).reversed())
                .toList();

        for (Layer layer : sortedLayers) {
            layer.unfreezeLayer();
        }
    }

    public void close() {
        try {
            layerAdapter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        virtualThreadRunner.shutdown(10, TimeUnit.SECONDS);
        platformThreadRunner.shutdown(10, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return "LayerRegistry";
    }

    private record LoopData(boolean virtual, String layer) {
    }

    ;
}
