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
import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.mgmt.MgmtSwiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.EventProcessor;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadpoolSwiftletImpl extends ThreadpoolSwiftlet
        implements TimerListener {
    public static final String PROP_COLLECT_INTERVAL = "collect-interval";
    public static final String PROP_PLATFORM_THREADS = "platform";
    public static final String PROP_VIRTUAL_THREADS = "virtual";
    public static final String PROP_ADHOC_VIRTUAL_THREADS = "adhocvirtual";
    public static final String PROP_ADHOC_PLATFORM_THREADS = "adhocplatform";

    SwiftletContext ctx = null;
    GroupRegistry groupRegistry = null;
    ThreadRunner adHocVirtualThreadRunner = null;
    ThreadRunner adHocPlatformThreadRunner = null;

    final AtomicBoolean collectOn = new AtomicBoolean(false);
    final AtomicLong collectInterval = new AtomicLong(-1);


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
        return runAsync(runnable, true);
    }

    @Override
    public CompletableFuture<?> runAsync(Runnable runnable, boolean virtual) {
        return virtual ? adHocVirtualThreadRunner.execute(runnable) : adHocPlatformThreadRunner.execute(runnable);
    }

    @Override
    public EventLoop createEventLoop(String id, EventProcessor eventProcessor) {
        EventLoopImpl eventLoop = new EventLoopImpl(ctx, id, groupRegistry.isBulkMode(id), eventProcessor, groupRegistry.threadRunnerForEventLoop(id));
        groupRegistry.getGroup(id).addEventLoop(eventLoop);
        return eventLoop;
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting thread counts...");
        try {
            Integer platformRunningCount = groupRegistry.platformThreads();
            Integer virtualRunningCount = groupRegistry.virtualThreads();
            Integer adHocVirtualRunningCount = adHocVirtualThreadRunner.getActiveThreadCount();
            Integer adHocPlatformRunningCount = adHocPlatformThreadRunner.getActiveThreadCount();
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
            prop = ctx.usage.getProperty(PROP_ADHOC_VIRTUAL_THREADS);
            oldValue = (Integer) prop.getValue();
            if (!Objects.equals(oldValue, adHocVirtualRunningCount)) {
                prop.setReadOnly(false);
                prop.setValue(adHocVirtualRunningCount);
                prop.setReadOnly(true);
            }
            prop = ctx.usage.getProperty(PROP_ADHOC_PLATFORM_THREADS);
            oldValue = (Integer) prop.getValue();
            if (!Objects.equals(oldValue, adHocPlatformRunningCount)) {
                prop.setReadOnly(false);
                prop.setValue(adHocPlatformRunningCount);
                prop.setReadOnly(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "collecting thread counts...DONE.");
    }

    @Override
    public void freeze() {
        try {
            groupRegistry.freezeGroups().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unfreeze() {
        groupRegistry.unfreezeGroups();
    }

    protected void startup(Configuration config) throws SwiftletException {
        ctx = new SwiftletContext(config, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        groupRegistry = new GroupRegistry(ctx);
        adHocVirtualThreadRunner = new VirtualThreadRunner();
        adHocPlatformThreadRunner = new PlatformThreadRunner((ThreadPoolExecutor) Executors.newCachedThreadPool());

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
            public void propertyChanged(Property property, Object oldValue, Object newValue) {
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
        ctx.logSwiftlet.logInformation(getName(), "shutdown/adHocVirtualThreadRunner shutdown");
        adHocVirtualThreadRunner.shutdown(10, TimeUnit.SECONDS);
        ctx.logSwiftlet.logInformation(getName(), "shutdown/adHocVirtualThreadRunner shutdown done");
        ctx.logSwiftlet.logInformation(getName(), "shutdown/adHocPlatformThreadRunner shutdown");
        adHocPlatformThreadRunner.shutdown(10, TimeUnit.SECONDS);
        ctx.logSwiftlet.logInformation(getName(), "shutdown/adHocPlatformThreadRunner shutdown done");
        groupRegistry.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done.");
    }
}

