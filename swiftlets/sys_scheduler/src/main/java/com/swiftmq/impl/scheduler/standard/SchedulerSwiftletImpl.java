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

import com.swiftmq.impl.scheduler.standard.job.MessageSenderJobFactory;
import com.swiftmq.impl.scheduler.standard.po.CalendarAdded;
import com.swiftmq.impl.scheduler.standard.po.CalendarRemoved;
import com.swiftmq.impl.scheduler.standard.po.ScheduleAdded;
import com.swiftmq.impl.scheduler.standard.po.ScheduleRemoved;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.scheduler.InvalidScheduleException;
import com.swiftmq.swiftlet.scheduler.JobFactory;
import com.swiftmq.swiftlet.scheduler.JobGroup;
import com.swiftmq.swiftlet.scheduler.SchedulerSwiftlet;
import com.swiftmq.tools.concurrent.Semaphore;

public class SchedulerSwiftletImpl extends SchedulerSwiftlet {
    SwiftletContext ctx = null;
    EntityListEventAdapter calendarAdapter = null;
    EntityListEventAdapter scheduleAdapter = null;
    JobGroup myJobGroup = null;

    public JobGroup getJobGroup(String s) {
        Entity entity = ctx.jobGroupList.getEntity(s);
        if (entity == null) {
            entity = ctx.jobGroupList.createEntity();
            entity.setName(s);
            entity.createCommands();
            entity.setDynamic(true);
            entity.setDynamicObject(new JobGroupImpl(ctx, s, (EntityList) entity.getEntity("jobs")));
            try {
                ctx.jobGroupList.addEntity(entity);
            } catch (EntityAddException e) {
            }
        }
        return (JobGroup) entity.getDynamicObject();
    }

    public void addTemporarySchedule(String name, String jobGroup,
                                     String jobName, String calendar,
                                     String dateFrom, String dateTo,
                                     String timeExpr, String maxRuntime, boolean loggingEnabled) throws InvalidScheduleException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addTemporarySchedule, name=" + name + " ...");
        Schedule schedule = ScheduleFactory.createSchedule(ctx, name, jobGroup, jobName, calendar, dateFrom, dateTo, timeExpr, maxRuntime, loggingEnabled);
        schedule.setEnabled(true);
        try {
            ctx.scheduler.enqueue(new ScheduleAdded(schedule.createCopy()));
        } catch (Exception e) {
            throw new InvalidScheduleException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "addTemporarySchedule, name=" + name + " done");
    }

    public boolean removeTemporarySchedule(String name) {
        boolean rc = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "removeTemporarySchedule, name=" + name + " ...");
        Semaphore sem = new Semaphore();
        ScheduleRemoved po = new ScheduleRemoved(name, sem);
        ctx.scheduler.enqueue(po);
        sem.waitHere(1000);
        rc = po.isSuccess();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "removeTemporarySchedule, name=" + name + " done (rc=" + rc + ")");
        return rc;
    }

    private void createCalendarAdapter(EntityList calendarList) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createCalendarAdapter ...");
        calendarAdapter = new EntityListEventAdapter(calendarList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    SchedulerCalendar cal = CalendarFactory.createCalendar(ctx, newEntity);
                    ctx.scheduler.enqueue(new CalendarAdded(cal.createCopy()));
                    newEntity.setUserObject(cal);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                ctx.scheduler.enqueue(new CalendarRemoved(delEntity.getName()));
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            calendarAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createCalendarAdapter done");
    }

    private void createScheduleAdapter(EntityList scheduleList) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createScheduleAdapter ...");
        scheduleAdapter = new EntityListEventAdapter(scheduleList, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    if (newEntity.getName().indexOf('.') != -1)
                        throw new Exception("Invalid name, '.' is reserved for internal use!");
                    Schedule schedule = ScheduleFactory.createSchedule(ctx, newEntity);
                    ctx.scheduler.enqueue(new ScheduleAdded(schedule.createCopy()));
                    newEntity.setUserObject(schedule);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                ctx.scheduler.enqueue(new ScheduleRemoved(delEntity.getName()));
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            scheduleAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createScheduleAdapter done");
    }

    protected void startup(Configuration config) throws SwiftletException {
        try {
            ctx = new SwiftletContext(this, config);
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        ctx.scheduler = new Scheduler(ctx);
        createCalendarAdapter((EntityList) ctx.root.getEntity("calendars"));
        createScheduleAdapter((EntityList) ctx.root.getEntity("schedules"));
        myJobGroup = getJobGroup(SwiftletContext.JOBGROUP);
        JobFactory jf = new MessageSenderJobFactory(ctx);
        myJobGroup.addJobFactory(jf.getName(), jf);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done");
    }

    protected void shutdown() throws SwiftletException {
        // true when shutdown while standby
        if (ctx == null)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        myJobGroup.removeAll();
        try {
            scheduleAdapter.close();
            calendarAdapter.close();
        } catch (Exception e) {
        }
        ctx.scheduler.close();
        try {
            ctx.close();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done");
        ctx = null;
    }
}
