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

package com.swiftmq.impl.threadpool.standard.group;

import com.swiftmq.impl.threadpool.standard.SwiftletContext;
import com.swiftmq.impl.threadpool.standard.group.pool.PlatformThreadRunner;
import com.swiftmq.impl.threadpool.standard.group.pool.ThreadRunner;
import com.swiftmq.impl.threadpool.standard.group.pool.VirtualThreadRunner;
import com.swiftmq.mgmt.*;
import com.swiftmq.util.SwiftUtilities;

import java.util.*;
import java.util.concurrent.*;

public class GroupRegistry {
    private static final String DEFAULT_LAYER_ID = "default";
    private final SwiftletContext ctx;
    private final Map<String, Group> groups;
    private final EntityList groupList;
    private EntityListEventAdapter groupAdapter = null;
    private final ThreadRunner platformThreadRunner = new PlatformThreadRunner((ThreadPoolExecutor) Executors.newCachedThreadPool());
    private final ThreadRunner virtualThreadRunner = new VirtualThreadRunner();
    private final Map<String, LoopData> eventLoopConfig = new ConcurrentHashMap<>();
    private final String tracePrefix;

    public GroupRegistry(SwiftletContext ctx) {
        this.ctx = ctx;
        this.tracePrefix = ctx.threadpoolSwiftlet.getName();
        this.groupList = (EntityList) ctx.config.getEntity("groups");
        this.groups = new ConcurrentHashMap<>();
        registerGroup(new Group(DEFAULT_LAYER_ID, true));
        createGroups();
    }

    private void createGroups() {
        groupAdapter = new EntityListEventAdapter(groupList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                try {
                    final String groupName = newEntity.getName();
                    registerGroup(new Group(groupName, (Boolean) newEntity.getProperty("freezable").getValue()));
                    EntityListEventAdapter loopAdapter = new EntityListEventAdapter((EntityList) newEntity.getEntity("eventloops"), true, true) {
                        @Override
                        public void onEntityAdd(Entity parent, Entity newEntity) {
                            eventLoopConfig.put(newEntity.getName(), new LoopData((Boolean) newEntity.getProperty("virtual").getValue(), (Boolean) newEntity.getProperty("bulk-mode").getValue(), groupName));
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
                deregisterGroup(delEntity.getName());
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
            groupAdapter.init();
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
    }

    public ThreadRunner threadRunnerForEventLoop(String id) {
        ThreadRunner runner = platformThreadRunner;
        LoopData record = eventLoopConfig.get(id);
        if (record == null)
            ctx.logSwiftlet.logInformation(tracePrefix, this + "/threadRunnerForEventLoop, event loop for id=" + id + " is not configured, using platformThreadRunner");
        else {
            if (record.virtual())
                runner = virtualThreadRunner;
        }
        return runner;
    }

    public void registerGroup(Group group) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, this + "/registerGroup, group=" + group.getName());
        groups.put(group.getName(), group);
    }

    public void deregisterGroup(String identifier) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, this + "/deregisterGroup, identifier=" + identifier);
        Group group = groups.remove(identifier);
        if (group != null)
            group.close();
    }

    public Group getGroup(String loopName) {
        String groupName;
        if (eventLoopConfig.get(loopName) != null)
            groupName = eventLoopConfig.get(loopName).group();
        else {
            groupName = DEFAULT_LAYER_ID;
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(tracePrefix, this + "/getGroup, loopName=" + loopName + " returns=" + groupName);
        return groups.get(groupName);
    }

    public boolean isBulkMode(String loopName) {
        boolean bulkMode = true;
        if (eventLoopConfig.get(loopName) != null)
            bulkMode = eventLoopConfig.get(loopName).bulkMode();
        return bulkMode;
    }

    public int platformThreads() {
        return platformThreadRunner.getActiveThreadCount();
    }

    public int virtualThreads() {
        return virtualThreadRunner.getActiveThreadCount();
    }

    public CompletableFuture<Void> freezeGroups() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, this + "/freezeGroups");
        var groupOrderList = getGroupOrderList();

        long freezableGroupCount = groupOrderList.stream()
                .map(groups::get)
                .filter(Objects::nonNull)
                .filter(Group::isFreezable)
                .count();

        CompletableFuture[] futures = new CompletableFuture[(int) freezableGroupCount];
        int index = 0;

        for (String name : groupOrderList) {
            Group group = groups.get(name);
            if (group == null)
                ctx.logSwiftlet.logError(tracePrefix, this + "/freezeGroups/unknown group in group-shutdown-order: " + name);
            else {
                if (group.isFreezable()) {
                    CompletableFuture<Void> groupFreezeFuture = new CompletableFuture<>();
                    futures[index++] = groupFreezeFuture;
                    ctx.logSwiftlet.logInformation(tracePrefix, this + "/freezeGroups/group " + name);
                    group.freezeGroup()
                            .thenRun(() -> ctx.logSwiftlet.logInformation(tracePrefix, this + "/freezeGroups/group " + name + " done"))
                            .thenRun(() -> groupFreezeFuture.complete(null));
                }
            }
        }

        return CompletableFuture.allOf(futures);
    }

    public void unfreezeGroups() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(tracePrefix, this + "/unfreezeGroups");
        var groupOrderList = getGroupOrderList();
        Collections.reverse(groupOrderList);

        for (String name : groupOrderList) {
            Group group = groups.get(name);
            if (group == null)
                ctx.logSwiftlet.logError(tracePrefix, this + "/unfreezeGroups/unknown group in group-shutdown-order: " + name);
            else {
                if (group.isFreezable()) {
                    ctx.logSwiftlet.logInformation(tracePrefix, this + "/unfreezeGroups/group " + name);
                    group.unfreezeGroup();
                    ctx.logSwiftlet.logInformation(tracePrefix, this + "/unfreezeGroups/group " + name + " done");
                }
            }
        }
    }

    private void shutdownGroups() {
        getGroupOrderList().forEach(name -> {
            Group group = groups.get(name);
            if (group == null)
                ctx.logSwiftlet.logError(tracePrefix, this + "/shutdownGroups/unknown group in group-shutdown-order: " + name);
            else {
                ctx.logSwiftlet.logInformation(tracePrefix, this + "/shutdownGroups/group " + name);
                group.close();
                ctx.logSwiftlet.logInformation(tracePrefix, this + "/shutdownGroups/group " + name + " done");
            }
        });
    }

    private List<String> getGroupOrderList() {
        return Arrays.stream(SwiftUtilities.tokenize((String) ctx.config.getProperty("group-shutdown-order").getValue(), " ")).toList();
    }

    public void close() {
        shutdownGroups();
        try {
            groupAdapter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ctx.logSwiftlet.logInformation(tracePrefix, this + "/close/virtualThreadRunner shutdown");
        virtualThreadRunner.shutdown(10, TimeUnit.SECONDS);
        ctx.logSwiftlet.logInformation(tracePrefix, this + "/close/virtualThreadRunner shutdown done");
        ctx.logSwiftlet.logInformation(tracePrefix, this + "/close/platformThreadRunner shutdown");
        platformThreadRunner.shutdown(10, TimeUnit.SECONDS);
        ctx.logSwiftlet.logInformation(tracePrefix, this + "/close/platformThreadRunner shutdown done");
    }

    @Override
    public String toString() {
        return "GroupRegistry";
    }

    private record LoopData(boolean virtual, boolean bulkMode, String group) {
    }

    ;
}
