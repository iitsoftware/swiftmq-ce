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

package com.swiftmq.impl.threadpool.standard;

import com.swiftmq.impl.threadpool.standard.group.EventLoopImpl;
import com.swiftmq.impl.threadpool.standard.group.GroupRegistry;
import com.swiftmq.impl.threadpool.standard.group.pool.PlatformThreadRunner;
import com.swiftmq.impl.threadpool.standard.group.pool.ThreadRunner;
import com.swiftmq.impl.threadpool.standard.group.pool.VirtualThreadRunner;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.threadpool.*;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadpoolSwiftletImpl extends ThreadpoolSwiftlet
        implements TimerListener {
    public static final String PROP_KERNEL_POOL = "kernel-pool";
    public static final String PROP_COLLECT_INTERVAL = "collect-interval";
    public static final String PROP_PLATFORM_THREADS = "01-platform";
    public static final String PROP_VIRTUAL_THREADS = "02-virtual";
    public static final String PROP_ADHOC_THREADS = "03-adhoc";
    public static final String DEFAULT_POOL = "default";

    SwiftletContext ctx = null;
    Map<String, ThreadPool> pools = new ConcurrentHashMap<>();
    Map<String, String> threadNameMaps = new ConcurrentHashMap<>();
    GroupRegistry groupRegistry = null;
    ThreadRunner adHocThreadRunner = null;

    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);
    final AtomicBoolean stopped = new AtomicBoolean(false);

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn.get())
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "collectChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: removeTimerListener for interval " + oldInterval);
            ctx.timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: addTimerListener for interval " + newInterval);
            ctx.timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    @Override
    public CompletableFuture<?> runAsync(Runnable runnable) {
        return adHocThreadRunner.execute(runnable);
    }

    @Override
    public EventLoop createEventLoop(String id, EventProcessor eventProcessor, boolean bulkMode) {
        EventLoopImpl eventLoop = new EventLoopImpl(id, bulkMode, eventProcessor, groupRegistry.threadRunnerForEventLoop(id));
        groupRegistry.getGroup(id).addEventLoop(eventLoop);
        return eventLoop;
    }

    private String[] getDefinedPoolnames(EntityList list) {
        if (list.getEntities().isEmpty())
            return null;
        return (String[]) list.getEntities().keySet().toArray(new String[0]);
    }

    private void storeThreadNamesForPool(String poolname, Entity poolEntity) {
        EntityList list = (EntityList) poolEntity.getEntity("threads");
        Map m = list.getEntities();
        if (!m.isEmpty()) {
            for (Object o : m.keySet())
                threadNameMaps.put((String) o, poolname);
        }
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting thread counts...");
        try {
            Integer platformRunningCount = groupRegistry.platformThreads();
            Integer virtualRunningCount = groupRegistry.virtualThreads();
            Integer adHocRunningCount = adHocThreadRunner.getActiveThreadCount();
            Property prop = ctx.usage.getProperty(PROP_PLATFORM_THREADS);
            Integer oldValue = (Integer) prop.getValue();
            if (!Objects.equals(oldValue, platformRunningCount)) {
                prop.setReadOnly(false);
                prop.setValue(platformRunningCount);
                prop.setReadOnly(true);
            }
            prop = ctx.usage.getProperty(PROP_VIRTUAL_THREADS);
            oldValue = (Integer) prop.getValue();
            if (!Objects.equals(oldValue, virtualRunningCount)) {
                prop.setReadOnly(false);
                prop.setValue(virtualRunningCount);
                prop.setReadOnly(true);
            }
            prop = ctx.usage.getProperty(PROP_ADHOC_THREADS);
            oldValue = (Integer) prop.getValue();
            if (!Objects.equals(oldValue, adHocRunningCount)) {
                prop.setReadOnly(false);
                prop.setValue(adHocRunningCount);
                prop.setReadOnly(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting thread counts...DONE.");
    }

    public String[] getPoolnames() {
        return pools.keySet().toArray(new String[0]);
    }

    public ThreadPool getPoolByName(String name) {
        return pools.get(name);
    }

    public ThreadPool getPool(String threadName) {
        String name = DEFAULT_POOL;
        for (String predicate : threadNameMaps.keySet()) {
            if (LikeComparator.compare(threadName, predicate, '\\')) {
                name = threadNameMaps.get(predicate);
                break;
            }
        }
        ThreadPool p = pools.get(name);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "getPoolForThreadName '" + threadName + "' returns " + name);
        return p;
    }

    public void dispatchTask(AsyncTask asyncTask) {
        ThreadPool pool = getPool(asyncTask.getDispatchToken());
        if (pool == null)
            return; // only during shutdown
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "dispatchTask, dispatchToken=" + asyncTask.getDispatchToken() +
                    ", description=" + asyncTask.getDescription() +
                    ", pool=" + pool.getPoolName());
        pool.dispatchTask(asyncTask);
    }

    public void stopPools() {
        if (stopped.getAndSet(true))
            return;
        for (Map.Entry<String, ThreadPool> stringThreadPoolEntry : pools.entrySet()) {
            ThreadPool p = (ThreadPool) ((Map.Entry<?, ?>) stringThreadPoolEntry).getValue();
            p.stop();
        }
    }

    private void createPoolChangeListeners(PoolDispatcher pool, Entity poolEntity) {
        Property prop = poolEntity.getProperty("max-threads");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                PoolDispatcher p = (PoolDispatcher) configObject;
                int n = (Integer) newValue;
                if (n < p.getMinThreads())
                    throw new PropertyChangeException("max-threads must be greater or equal to min-threads");
                p.setMaxThreads(n);
            }
        });
        prop = poolEntity.getProperty("queue-length-threshold");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                PoolDispatcher p = (PoolDispatcher) configObject;
                int n = (Integer) newValue;
                p.setThreshold(n);
            }
        });
        prop = poolEntity.getProperty("additional-threads");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                PoolDispatcher p = (PoolDispatcher) configObject;
                int n = (Integer) newValue;
                p.setAddThreads(n);
            }
        });
    }

    private void createPool(String poolName, Entity poolEntity, Entity defaultEntity) {
        Entity entity = poolEntity != null ? poolEntity : defaultEntity;
        boolean kernelPool = (Boolean) entity.getProperty("kernel-pool").getValue();
        int min = (Integer) entity.getProperty("min-threads").getValue();
        int max = (Integer) entity.getProperty("max-threads").getValue();
        int threshold = (Integer) entity.getProperty("queue-length-threshold").getValue();
        int addThreads = (Integer) entity.getProperty("additional-threads").getValue();
        int prio = (Integer) entity.getProperty("priority").getValue();
        long ttl = (Long) entity.getProperty("idle-timeout").getValue();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "creating thread pool '" + poolName +
                    "', kernelPool=" + kernelPool +
                    ", minThreads=" + min +
                    ", maxThreads=" + max +
                    ", threshold=" + threshold +
                    ", addThreads=" + addThreads +
                    ", prio=" + prio +
                    ", idletimeout=" + ttl);
        PoolDispatcher pool = new PoolDispatcher(getName(), poolName, kernelPool, min, max, threshold, addThreads, prio, ttl);
        pools.put(poolName, pool);
        createPoolChangeListeners(pool, entity);
        if (!poolName.equals(DEFAULT_POOL)) {
            EntityList list = (EntityList) poolEntity.getEntity("threads");
            list.setEntityAddListener(new EntityChangeAdapter(pool) {
                public void onEntityAdd(Entity parent, Entity newEntity)
                        throws EntityAddException {
                    PoolDispatcher myPd = (PoolDispatcher) configObject;
                    if (myPd.isKernelPool())
                        throw new EntityAddException("You cannot create a thread assignment for a kernel pool dynamically.");
                    threadNameMaps.put(newEntity.getName(), myPd.getPoolName());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityAdd (thread), poolName=" + myPd.getPoolName() + ", thread=" + newEntity.getName());
                }
            });
            list.setEntityRemoveListener(new EntityChangeAdapter(pool) {
                public void onEntityRemove(Entity parent, Entity delEntity)
                        throws EntityRemoveException {
                    PoolDispatcher myPd = (PoolDispatcher) configObject;
                    if (myPd.isKernelPool())
                        throw new EntityRemoveException("You cannot remove a thread assignment from a kernel pool dynamically.");
                    threadNameMaps.remove(delEntity.getName());
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(getName(), "onEntityRemove (thread), poolName=" + myPd.getPoolName() + ", thread=" + delEntity.getName());
                }
            });
        }
    }

    protected void startup(Configuration config) throws SwiftletException {
        ctx = new SwiftletContext(config, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        groupRegistry = new GroupRegistry(ctx);
        adHocThreadRunner = ((Boolean) ctx.config.getProperty("adhoc-runner-virtual").getValue()) ? new VirtualThreadRunner() : new PlatformThreadRunner();

        EntityList poolList = (EntityList) ctx.config.getEntity("pools");
        createPool(DEFAULT_POOL, null, poolList.getTemplate());
        PoolDispatcher dp = (PoolDispatcher) pools.get(DEFAULT_POOL);
        dp.setKernelPool(true);

        String[] poolNames = getDefinedPoolnames(poolList);
        if (poolNames != null && poolNames.length > 0) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup: starting defined thread pools");
            for (String poolName : poolNames) {
                createPool(poolName, poolList.getEntity(poolName), poolList.getTemplate());
                storeThreadNamesForPool(poolName, poolList.getEntity(poolName));
            }
        }
        poolList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                boolean kp = (Boolean) newEntity.getProperty(PROP_KERNEL_POOL).getValue();
                if (kp)
                    throw new EntityAddException("You cannot create a kernel pool dynamically.");
                createPool(newEntity.getName(), newEntity, newEntity);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd (pool), poolName=" + newEntity.getName());
            }
        });
        poolList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                PoolDispatcher pd = (PoolDispatcher) pools.get(delEntity.getName());
                if (pd.isKernelPool())
                    throw new EntityRemoveException("You cannot remove a kernel pool dynamically.");
                pd.close();
                pools.remove(delEntity.getName());
                poolList.removeDynamicEntity(pd);
                for (Iterator<Map.Entry<String, String>> iter = threadNameMaps.entrySet().iterator(); iter.hasNext(); ) {
                    String entry = (String) ((Map.Entry<?, ?>) iter.next()).getValue();
                    if (entry.equals(pd.getPoolName()))
                        iter.remove();
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove (pool): poolName=" + delEntity.getName());
            }
        });

        try {
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent evt) {
                    try {
                        ctx.timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
                        ctx.mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "registering MgmtListener ...");
                        ctx.mgmtSwiftlet.addMgmtListener(new MgmtListener() {
                            public void adminToolActivated() {
                                collectOn.set(true);
                                collectChanged(-1, collectInterval.get());
                            }

                            public void adminToolDeactivated() {
                                collectChanged(collectInterval.get(), -1);
                                collectOn.set(false);
                            }
                        });
                    } catch (Exception e) {
                        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                    }
                }
            });
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        Property prop = ctx.config.getProperty(PROP_COLLECT_INTERVAL);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                collectInterval.set((Long) newValue);
                collectChanged((Long) oldValue, collectInterval.get());
            }
        });
        collectInterval.set((Long) prop.getValue());
        if (collectOn.get()) {
            if (collectInterval.get() > 0) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "startup: registering thread count collector");
                ctx.timerSwiftlet.addTimerListener(collectInterval.get(), this);
            } else if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "startup: collect interval <= 0; no thread count collector");
        }
    }

    protected void shutdown() {
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: closing thread pools ...");
        for (Map.Entry<String, ThreadPool> entry : pools.entrySet()) {
            ThreadPool p = (ThreadPool) ((Map.Entry<?, ?>) entry).getValue();
            p.close();
        }
        pools.clear();
        threadNameMaps.clear();
        groupRegistry.close();
        ctx.logSwiftlet.logInformation(getName(), "/shutdown/adHocThreadRunner shutdown");
        adHocThreadRunner.shutdown(10, TimeUnit.SECONDS);
        ctx.logSwiftlet.logInformation(getName(), "/shutdown/adHocThreadRunner shutdown done");
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done.");
    }
}

