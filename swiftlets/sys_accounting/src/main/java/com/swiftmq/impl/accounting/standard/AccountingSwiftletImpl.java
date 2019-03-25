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

package com.swiftmq.impl.accounting.standard;

import com.swiftmq.impl.accounting.standard.factoryimpl.FileSinkFactory;
import com.swiftmq.impl.accounting.standard.factoryimpl.JDBCSinkFactory;
import com.swiftmq.impl.accounting.standard.factoryimpl.QueueSinkFactory;
import com.swiftmq.impl.accounting.standard.factoryimpl.QueueSourceFactory;
import com.swiftmq.impl.accounting.standard.jobs.JobRegistrar;
import com.swiftmq.impl.accounting.standard.po.*;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.accounting.AccountingSinkFactory;
import com.swiftmq.swiftlet.accounting.AccountingSourceFactory;
import com.swiftmq.swiftlet.accounting.AccountingSwiftlet;
import com.swiftmq.swiftlet.event.SwiftletManagerAdapter;
import com.swiftmq.swiftlet.event.SwiftletManagerEvent;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.tools.concurrent.Semaphore;

public class AccountingSwiftletImpl extends AccountingSwiftlet {
    SwiftletContext ctx = null;
    EntityListEventAdapter connectionAdapter = null;
    AccountingSourceFactory queueSourceFactory = null;
    AccountingSinkFactory queueSinkFactory = null;
    AccountingSinkFactory fileSinkFactory = null;
    AccountingSinkFactory jdbcSinkFactory = null;
    JobRegistrar jobRegistrar = null;

    private void createConnectionAdapter(EntityList connectionList) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createConnectionAdapter ...");
        connectionAdapter = new EntityListEventAdapter(connectionList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                Semaphore sem = new Semaphore();
                ConnectionAdded po = new ConnectionAdded(sem, newEntity);
                ctx.eventProcessor.enqueue(po);
                sem.waitHere();
                if (!po.isSuccess())
                    throw new EntityAddException(po.getException());
                Property p = newEntity.getProperty("enabled");
                p.setPropertyChangeListener(new PropertyChangeListener() {
                    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
                        Semaphore sem2 = new Semaphore();
                        ConnectionStateChanged po2 = new ConnectionStateChanged(sem2, property.getParent(), ((Boolean) newValue).booleanValue());
                        ctx.eventProcessor.enqueue(po2);
                        sem2.waitHere();
                        if (!po2.isSuccess())
                            throw new PropertyChangeException(po2.getException());
                    }
                });
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                Semaphore sem = new Semaphore();
                ConnectionRemoved po = new ConnectionRemoved(sem, delEntity.getName());
                ctx.eventProcessor.enqueue(po);
                sem.waitHere();
                if (!po.isSuccess())
                    throw new EntityRemoveException(po.getException());
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            connectionAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createConnectionAdapter done");
    }

    public void addAccountingSourceFactory(String group, String name, AccountingSourceFactory factory) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addAccountingSourceFactory, group=" + group + ", name=" + name + " ...");
        Semaphore sem = new Semaphore();
        ctx.eventProcessor.enqueue(new SourceFactoryAdded(sem, group, name, factory));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addAccountingSourceFactory done.");
    }

    public void removeAccountingSourceFactory(String group, String name) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeAccountingSourceFactory, group=" + group + ", name=" + name + " ...");
        Semaphore sem = new Semaphore();
        ctx.eventProcessor.enqueue(new SourceFactoryRemoved(sem, group, name));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeAccountingSourceFactory done.");
    }

    public void addAccountingSinkFactory(String group, String name, AccountingSinkFactory factory) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "addAccountingSinkFactory, group=" + group + ", name=" + name + " ...");
        Semaphore sem = new Semaphore();
        ctx.eventProcessor.enqueue(new SinkFactoryAdded(sem, group, name, factory));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addAccountingSinkFactory done.");
    }

    public void removeAccountingSinkFactory(String group, String name) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeAccountingSinkFactory, group=" + group + ", name=" + name + " ...");
        Semaphore sem = new Semaphore();
        ctx.eventProcessor.enqueue(new SinkFactoryRemoved(sem, group, name));
        sem.waitHere();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeAccountingSinkFactory done.");
    }

    protected void startup(Configuration configuration) throws SwiftletException {
        ctx = new SwiftletContext(configuration, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

        createConnectionAdapter((EntityList) ctx.root.getEntity("connections"));

        queueSourceFactory = new QueueSourceFactory(ctx);
        addAccountingSourceFactory(queueSourceFactory.getGroup(), queueSourceFactory.getName(), queueSourceFactory);

        queueSinkFactory = new QueueSinkFactory(ctx);
        addAccountingSinkFactory(queueSinkFactory.getGroup(), queueSinkFactory.getName(), queueSinkFactory);

        fileSinkFactory = new FileSinkFactory(ctx);
        addAccountingSinkFactory(fileSinkFactory.getGroup(), fileSinkFactory.getName(), fileSinkFactory);

        jdbcSinkFactory = new JDBCSinkFactory(ctx);
        addAccountingSinkFactory(jdbcSinkFactory.getGroup(), jdbcSinkFactory.getName(), jdbcSinkFactory);

        SwiftletManager.getInstance().addSwiftletManagerListener("sys$scheduler", new SwiftletManagerAdapter() {
            public void swiftletStarted(SwiftletManagerEvent event) {
                ctx.schedulerSwiftlet = (SchedulerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$scheduler");
                jobRegistrar = new JobRegistrar(ctx);
                jobRegistrar.register();
            }

            public void swiftletStopInitiated(SwiftletManagerEvent event) {
                jobRegistrar.unregister();
                ctx.schedulerSwiftlet = null;
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
    }

    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        removeAccountingSourceFactory(queueSourceFactory.getGroup(), queueSourceFactory.getName());
        removeAccountingSinkFactory(queueSinkFactory.getGroup(), queueSinkFactory.getName());
        removeAccountingSinkFactory(fileSinkFactory.getGroup(), fileSinkFactory.getName());
        removeAccountingSinkFactory(jdbcSinkFactory.getGroup(), jdbcSinkFactory.getName());
        try {
            connectionAdapter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ctx.eventProcessor.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
        ctx = null;
    }
}
