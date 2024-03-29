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

package com.swiftmq.impl.scheduler.standard;

import com.swiftmq.impl.scheduler.standard.po.JobFactoryAdded;
import com.swiftmq.impl.scheduler.standard.po.JobFactoryRemoved;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;
import com.swiftmq.swiftlet.scheduler.JobParameter;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JobGroupImpl implements JobGroup {
    SwiftletContext ctx = null;
    String name = null;
    EntityList jobList = null;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public JobGroupImpl(SwiftletContext ctx, String name, EntityList jobList) {
        this.ctx = ctx;
        this.name = name;
        this.jobList = jobList;
    }

    public String getName() {
        return name;
    }

    public boolean hasJobFactory(String name) {
        lock.readLock().lock();
        try {
            return jobList.getEntity(name) != null;
        } finally {
            lock.readLock().unlock();
        }

    }

    public String[] getJobFactoryNames() {
        lock.readLock().lock();
        try {
            return jobList.getEntityNames();
        } finally {
            lock.readLock().unlock();
        }

    }

    public void addJobFactory(String jobName, JobFactory jobFactory) {
        lock.writeLock().lock();
        try {
            if (!hasJobFactory(jobName)) {
                Entity entity = jobList.createEntity();
                entity.setName(jobFactory.getName());
                entity.createCommands();
                entity.setDynamic(true);
                entity.setDynamicObject(jobFactory);
                try {
                    Property prop = entity.getProperty("description");
                    prop.setReadOnly(false);
                    prop.setValue(jobFactory.getDescription());
                    prop.setReadOnly(true);
                    Map parmMap = jobFactory.getJobParameters();
                    if (parmMap != null) {
                        EntityList parmList = (EntityList) entity.getEntity("parameters");
                        for (Iterator iter = parmMap.entrySet().iterator(); iter.hasNext(); ) {
                            JobParameter parm = (JobParameter) ((Map.Entry) iter.next()).getValue();
                            Entity pe = parmList.createEntity();
                            pe.setName(parm.getName());
                            pe.setDynamic(true);
                            pe.createCommands();
                            pe.setDynamicObject(parm);
                            prop = pe.getProperty("description");
                            prop.setReadOnly(false);
                            prop.setValue(parm.getDescription());
                            prop.setReadOnly(true);
                            prop = pe.getProperty("default");
                            prop.setReadOnly(false);
                            prop.setValue(parm.getDefaultValue());
                            prop.setReadOnly(true);
                            prop = pe.getProperty("mandatory");
                            prop.setReadOnly(false);
                            prop.setValue(new Boolean(parm.isMandatory()));
                            prop.setReadOnly(true);
                            parmList.addEntity(pe);
                        }
                    }
                    jobList.addEntity(entity);
                    ctx.scheduler.enqueue(new JobFactoryAdded(name, jobName, jobFactory));
                } catch (Exception e) {
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeJobFactory(String jobName) {
        lock.writeLock().lock();
        try {
            Entity jobEntity = jobList.getEntity(jobName);
            if (jobEntity != null) {
                try {
                    jobList.removeEntity(jobEntity);
                } catch (EntityRemoveException e) {
                }
                ctx.scheduler.enqueue(new JobFactoryRemoved(name, jobName));
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeAll() {
        lock.writeLock().lock();
        try {
            String[] names = jobList.getEntityNames();
            if (names != null) {
                for (String s : names) {
                    removeJobFactory(s);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }
}
