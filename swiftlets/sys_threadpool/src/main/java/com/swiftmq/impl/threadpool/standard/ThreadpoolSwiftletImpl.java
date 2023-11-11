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

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ThreadpoolSwiftletImpl extends ThreadpoolSwiftlet
        implements TimerListener {
    public static final String PROP_KERNEL_POOL = "kernel-pool";
    public static final String PROP_COLLECT_INTERVAL = "collect-interval";
    public static final String PROP_THREADS_IDLING = "idling-threads";
    public static final String PROP_THREADS_RUNNING = "running-threads";
    public static final String DEFAULT_POOL = "default";

    Configuration config = null;
    Entity root = null;
    EntityList usageList = null;

    MgmtSwiftlet mgmtSwiftlet = null;
    TimerSwiftlet timerSwiftlet = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;

    HashMap pools = new HashMap();
    HashMap threadNameMaps = new HashMap();

    boolean collectOn = false;
    long collectInterval = -1;
    boolean stopped = false;

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn)
            return;
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "collectChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "collectChanged: removeTimerListener for interval " + oldInterval);
            timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "collectChanged: addTimerListener for interval " + newInterval);
            timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    private String[] getDefinedPoolnames(EntityList list) {
        Map m = list.getEntities();
        if (m.size() == 0)
            return null;
        String[] rArray = new String[m.size()];
        int i = 0;
        for (Iterator iter = m.keySet().iterator(); iter.hasNext(); )
            rArray[i++] = (String) iter.next();
        return rArray;
    }

    private void storeThreadNamesForPool(String poolname, Entity poolEntity) {
        EntityList list = (EntityList) poolEntity.getEntity("threads");
        Map m = list.getEntities();
        if (m.size() > 0) {
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); )
                threadNameMaps.put((String) iter.next(), poolname);
        }
    }

    public void performTimeAction() {
        if (traceSpace.enabled) traceSpace.trace(getName(), "collecting thread counts...");
        synchronized (pools) {
            for (Iterator iter = pools.keySet().iterator(); iter.hasNext(); ) {
                try {
                    String name = (String) iter.next();
                    ThreadPool p = (ThreadPool) pools.get(name);
                    int idleCount = p.getNumberIdlingThreads();
                    int runningCount = p.getNumberRunningThreads();
                    Entity tpEntity = usageList.getEntity(name);
                    Property prop = tpEntity.getProperty(PROP_THREADS_IDLING);
                    int oldValue = (int) ((Integer) prop.getValue()).intValue();
                    if (oldValue != idleCount) {
                        prop.setReadOnly(false);
                        prop.setValue(new Integer(idleCount));
                        prop.setReadOnly(true);
                    }
                    prop = tpEntity.getProperty(PROP_THREADS_RUNNING);
                    oldValue = (int) ((Integer) prop.getValue()).intValue();
                    if (oldValue != runningCount) {
                        prop.setReadOnly(false);
                        prop.setValue(new Integer(runningCount));
                        prop.setReadOnly(true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (traceSpace.enabled) traceSpace.trace(getName(), "collecting thread counts...DONE.");
    }

    public String[] getPoolnames() {
        String[] names = null;
        synchronized (pools) {
            names = new String[pools.size()];
            int i = 0;
            Iterator iter = pools.keySet().iterator();
            while (iter.hasNext())
                names[i++] = (String) iter.next();
        }
        return names;
    }

    public ThreadPool getPoolByName(String name) {
        return (ThreadPool) pools.get(name);
    }

    public ThreadPool getPool(String threadName) {
        String name = DEFAULT_POOL;
        synchronized (threadNameMaps) {
            Iterator iter = threadNameMaps.keySet().iterator();
            while (iter.hasNext()) {
                String predicate = (String) iter.next();
                if (LikeComparator.compare(threadName, predicate, '\\')) {
                    name = (String) threadNameMaps.get(predicate);
                    break;
                }
            }
        }
        ThreadPool p = (ThreadPool) pools.get(name);
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "getPoolForThreadName '" + threadName + "' returns " + name);
        return p;
    }

    /**
     * @param asyncTask
     */
    public void dispatchTask(AsyncTask asyncTask) {
        ThreadPool pool = getPool(asyncTask.getDispatchToken());
        if (pool == null)
            return; // only during shutdown
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "dispatchTask, dispatchToken=" + asyncTask.getDispatchToken() +
                    ", description=" + asyncTask.getDescription() +
                    ", pool=" + pool.getPoolName());
        pool.dispatchTask(asyncTask);
    }

    public void stopPools() {
        synchronized (pools) {
            if (stopped)
                return;
            stopped = true;
            synchronized (pools) {
                for (Iterator iter = pools.entrySet().iterator(); iter.hasNext(); ) {
                    ThreadPool p = (ThreadPool) ((Map.Entry) iter.next()).getValue();
                    p.stop();
                }
            }
        }
    }

    private void createPoolChangeListeners(Pool pool, Entity poolEntity) {
        Property prop = poolEntity.getProperty("max-threads");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                Pool p = (Pool) configObject;
                int n = ((Integer) newValue).intValue();
                if (n < p.getMinThreads())
                    throw new PropertyChangeException("max-threads must be greater or equal to min-threads");
                p.setMaxThreads(n);
            }
        });
        prop = poolEntity.getProperty("queue-length-threshold");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                Pool p = (Pool) configObject;
                int n = ((Integer) newValue).intValue();
                p.setThreshold(n);
            }
        });
        prop = poolEntity.getProperty("additional-threads");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(pool) {

            public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                Pool p = (Pool) configObject;
                int n = ((Integer) newValue).intValue();
                p.setAddThreads(n);
            }
        });
    }

    private void createPool(String poolName, Entity poolEntity, Entity defaultEntity) {
        synchronized (pools) {
            Entity entity = poolEntity != null ? poolEntity : defaultEntity;
            boolean kernelPool = ((Boolean) entity.getProperty("kernel-pool").getValue()).booleanValue();
            int min = ((Integer) entity.getProperty("min-threads").getValue()).intValue();
            int max = ((Integer) entity.getProperty("max-threads").getValue()).intValue();
            int threshold = ((Integer) entity.getProperty("queue-length-threshold").getValue()).intValue();
            int addThreads = ((Integer) entity.getProperty("additional-threads").getValue()).intValue();
            int prio = ((Integer) entity.getProperty("priority").getValue()).intValue();
            long ttl = ((Long) entity.getProperty("idle-timeout").getValue()).longValue();
            if (traceSpace.enabled)
                traceSpace.trace(getName(), "creating thread pool '" + poolName +
                        "', kernelPool=" + kernelPool +
                        ", minThreads=" + min +
                        ", maxThreads=" + max +
                        ", threshold=" + threshold +
                        ", addThreads=" + addThreads +
                        ", prio=" + prio +
                        ", idletimeout=" + ttl);
            Pool pool = new Pool(getName(), poolName, kernelPool, min, max, threshold, addThreads, prio, ttl);
            pools.put(poolName, pool);
            Entity qEntity = usageList.createEntity();
            qEntity.setName(poolName);
            qEntity.setDynamicObject(pool);
            qEntity.createCommands();
            try {
                usageList.addEntity(qEntity);
            } catch (Exception ignored) {
            }
            createPoolChangeListeners(pool, entity);
            if (!poolName.equals(DEFAULT_POOL)) {
                EntityList list = (EntityList) poolEntity.getEntity("threads");
                list.setEntityAddListener(new EntityChangeAdapter(pool) {
                    public void onEntityAdd(Entity parent, Entity newEntity)
                            throws EntityAddException {
                        Pool myPd = (Pool) configObject;
                        if (myPd.isKernelPool())
                            throw new EntityAddException("You cannot create a thread assignment for a kernel pool dynamically.");
                        synchronized (threadNameMaps) {
                            threadNameMaps.put(newEntity.getName(), myPd.getPoolName());
                        }
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "onEntityAdd (thread), poolName=" + myPd.getPoolName() + ", thread=" + newEntity.getName());
                    }
                });
                list.setEntityRemoveListener(new EntityChangeAdapter(pool) {
                    public void onEntityRemove(Entity parent, Entity delEntity)
                            throws EntityRemoveException {
                        Pool myPd = (Pool) configObject;
                        if (myPd.isKernelPool())
                            throw new EntityRemoveException("You cannot remove a thread assignment from a kernel pool dynamically.");
                        synchronized (threadNameMaps) {
                            threadNameMaps.remove(delEntity.getName());
                        }
                        if (traceSpace.enabled)
                            traceSpace.trace(getName(), "onEntityRemove (thread), poolName=" + myPd.getPoolName() + ", thread=" + delEntity.getName());
                    }
                });
            }
        }
    }

    protected void startup(Configuration config) throws SwiftletException {
        this.config = config;
        root = config;
        usageList = (EntityList) root.getEntity("usage");

        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);

        if (traceSpace.enabled) traceSpace.trace(getName(), "startup ...");

        EntityList poolList = (EntityList) root.getEntity("pools");
        createPool(DEFAULT_POOL, null, poolList.getTemplate());
        Pool dp = (Pool) pools.get(DEFAULT_POOL);
        dp.setKernelPool(true);

        String[] poolNames = getDefinedPoolnames(poolList);
        if (poolNames != null && poolNames.length > 0) {
            if (traceSpace.enabled) traceSpace.trace(getName(), "startup: starting defined thread pools");
            for (int i = 0; i < poolNames.length; i++) {
                createPool(poolNames[i], poolList.getEntity(poolNames[i]), poolList.getTemplate());
                storeThreadNamesForPool(poolNames[i], poolList.getEntity(poolNames[i]));
            }
        }
        poolList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                boolean kp = ((Boolean) newEntity.getProperty(PROP_KERNEL_POOL).getValue()).booleanValue();
                if (kp)
                    throw new EntityAddException("You cannot create a kernel pool dynamically.");
                createPool(newEntity.getName(), newEntity, newEntity);
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityAdd (pool), poolName=" + newEntity.getName());
            }
        });
        poolList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Pool pd = null;
                synchronized (pools) {
                    pd = (Pool) pools.get(delEntity.getName());
                    if (pd.isKernelPool())
                        throw new EntityRemoveException("You cannot remove a kernel pool dynamically.");
                    pd.close();
                    pools.remove(delEntity.getName());
                    usageList.removeDynamicEntity(pd);
                }
                synchronized (threadNameMaps) {
                    for (Iterator iter = threadNameMaps.entrySet().iterator(); iter.hasNext(); ) {
                        String entry = (String) ((Map.Entry) iter.next()).getValue();
                        if (entry.equals(pd.getPoolName()))
                            iter.remove();
                    }
                }
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityRemove (pool): poolName=" + delEntity.getName());
            }
        });

        try {
            SwiftletManager.getInstance().addSwiftletManagerListener("sys$mgmt", new SwiftletManagerAdapter() {
                public void swiftletStarted(SwiftletManagerEvent evt) {
                    try {
                        timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
                        mgmtSwiftlet = (MgmtSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$mgmt");
                        if (traceSpace.enabled) traceSpace.trace(getName(), "registering MgmtListener ...");
                        mgmtSwiftlet.addMgmtListener(new MgmtListener() {
                            public void adminToolActivated() {
                                collectOn = true;
                                collectChanged(-1, collectInterval);
                            }

                            public void adminToolDeactivated() {
                                collectChanged(collectInterval, -1);
                                collectOn = false;
                            }
                        });
                    } catch (Exception e) {
                        if (traceSpace.enabled) traceSpace.trace(getName(), "swiftletStartet, exception=" + e);
                    }
                }
            });
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }

        Property prop = root.getProperty(PROP_COLLECT_INTERVAL);
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                collectInterval = ((Long) newValue).longValue();
                collectChanged(((Long) oldValue).longValue(), collectInterval);
            }
        });
        collectInterval = ((Long) prop.getValue()).longValue();
        if (collectOn) {
            if (collectInterval > 0) {
                if (traceSpace.enabled) traceSpace.trace(getName(), "startup: registering thread count collector");
                timerSwiftlet.addTimerListener(collectInterval, this);
            } else if (traceSpace.enabled)
                traceSpace.trace(getName(), "startup: collect interval <= 0; no thread count collector");
        }
    }

    protected void shutdown() throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: closing thread pools ...");
        synchronized (pools) {
            Iterator iter = pools.entrySet().iterator();
            while (iter.hasNext()) {
                ThreadPool p = (ThreadPool) ((Map.Entry) iter.next()).getValue();
                p.close();
            }
            pools.clear();
            threadNameMaps.clear();
        }
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: done.");
    }
}

