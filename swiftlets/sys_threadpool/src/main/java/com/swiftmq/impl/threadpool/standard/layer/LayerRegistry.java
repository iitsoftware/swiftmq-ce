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

import com.swiftmq.swiftlet.trace.TraceSpace;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class LayerRegistry {
    private static final String DEFAULT_LAYER_ID = "default";
    private static final int DEFAULT_LAYER_PRIORITY = 0;
    private final TraceSpace traceSpace;
    private final String tracePrefix;
    private final Map<String, Layer> layers;

    public LayerRegistry(TraceSpace traceSpace, String tracePrefix) {
        this.traceSpace = traceSpace;
        this.tracePrefix = tracePrefix + "/" + this;
        this.layers = new ConcurrentHashMap<>();
        registerLayer(new Layer(DEFAULT_LAYER_ID, DEFAULT_LAYER_PRIORITY));
    }

    public void registerLayer(Layer layer) {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "registerLayer, layer=" + layer);
        layers.put(layer.getIdentifier(), layer);
    }

    public void deregisterLayer(String identifier) {
        if (traceSpace.enabled) traceSpace.trace(tracePrefix, "deregisterLayer, identifier=" + identifier);
        layers.remove(identifier);
    }

    public Layer getLayer(String identifier) {
        return layers.computeIfAbsent(identifier, id -> {
            return layers.get(DEFAULT_LAYER_ID);
        });
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

    @Override
    public String toString() {
        return "LayerRegistry";
    }
}
