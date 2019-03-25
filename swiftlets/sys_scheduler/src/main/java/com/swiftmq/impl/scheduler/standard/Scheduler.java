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

import com.swiftmq.impl.scheduler.standard.po.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.swiftlet.scheduler.*;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.pipeline.POObject;
import com.swiftmq.tools.pipeline.PipelineQueue;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Scheduler
        implements EventVisitor {
    static final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static final DecimalFormat dfmt = new DecimalFormat("000");
    static final String TP_SCHEDULER = "sys$scheduler.scheduler";
    static final String TP_RUNNER = "sys$scheduler.runner";
    static final String DONOTHING = "DONOTHING";
    static final String STATE_NONE = "NONE";
    static final String STATE_SCHEDULED = "SCHEDULED";
    static final String STATE_RUNNING = "RUNNING";
    static final String STATE_STOPPED = "STOPPED";
    static final String STATE_INVALID = "INVALID";
    static final String STATE_DISABLED = "SCHEDULE DISABLED";
    static final String STATE_NO_JOBFACTORY = "WAITING FOR JOB REGISTRATION";
    static final String STATE_NO_FURTHER_SCHEDULE = "SCHEDULE EXPIRED";
    static final String STATE_JOB_EXCEPTION = "JOB ERROR";
    SwiftletContext ctx = null;
    PipelineQueue pipelineQueue = null;
    ThreadPool runnerPool = null;
    Map calendars = new HashMap();
    Map schedules = new HashMap();
    Map jobGroups = new HashMap();

    public Scheduler(SwiftletContext ctx) {
        this.ctx = ctx;
        runnerPool = ctx.threadpoolSwiftlet.getPool(TP_RUNNER);
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_SCHEDULER), TP_SCHEDULER, this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/created");
    }

    public void enqueue(POObject po) {
        pipelineQueue.enqueue(po);
    }

    private List getSchedules(String calendarName) {
        List list = new ArrayList();
        for (Iterator iter = schedules.entrySet().iterator(); iter.hasNext(); ) {
            Entry s = (Entry) ((Map.Entry) iter.next()).getValue();
            if (s.schedule.hasCalendarRef(calendarName, calendars))
                list.add(s);
        }
        return list;
    }

    private boolean hasJobFactory(Entry scheduleEntry) {
        Map factories = (Map) jobGroups.get(scheduleEntry.schedule.getJobGroup());
        if (factories == null)
            return false;
        return factories.get(scheduleEntry.schedule.getJobName()) != null;
    }

    private JobFactory getJobFactory(Entry scheduleEntry) {
        Map factories = (Map) jobGroups.get(scheduleEntry.schedule.getJobGroup());
        if (factories == null)
            return null;
        return (JobFactory) factories.get(scheduleEntry.schedule.getJobName());
    }

    private void calcSchedule(Entry scheduleEntry) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + " ...");
        try {
            if (hasJobFactory(scheduleEntry)) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + ", has job factory...");
                stopSchedule(scheduleEntry);
                scheduleEntry.jobStart = scheduleEntry.schedule.getNextJobStart(scheduleEntry.lastStartTime, Calendar.getInstance(), calendars);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + ", jobStart=" + scheduleEntry.jobStart);
                if (scheduleEntry.jobStart != null) {
                    scheduleEntry.lastStartTime = scheduleEntry.jobStart.getStartTimeSec();
                    scheduleEntry.entity.getProperty("state").setValue(STATE_SCHEDULED);
                    scheduleEntry.entity.getProperty("next-start").setValue(fmt.format(new Date(scheduleEntry.jobStart.getStartTime())));
                    scheduleEntry.entity.getProperty("next-stop").setValue(null);
                    addHistory(scheduleEntry, STATE_SCHEDULED, null);
                    scheduleEntry.jobStart.setScheduler(this);
                    ctx.timerSwiftlet.addInstantTimerListener(Math.max(scheduleEntry.jobStart.getStartTime() - System.currentTimeMillis(), 0), scheduleEntry.jobStart, !scheduleEntry.schedule.isApplySystemTimeChange());
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + ", no job start!");
                    String state = null;
                    if (scheduleEntry.schedule.isEnabled()) {
                        state = STATE_NO_FURTHER_SCHEDULE;
                        if (scheduleEntry.messageSchedule)
                            ctx.activeMessageScheduleList.removeEntity(scheduleEntry.entity);
                    } else
                        state = STATE_DISABLED;
                    scheduleEntry.entity.getProperty("state").setValue(state);
                    scheduleEntry.entity.getProperty("next-start").setValue(null);
                    scheduleEntry.entity.getProperty("next-stop").setValue(null);
                    addHistory(scheduleEntry, state, null);
                }
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + ", no job factory!");
                scheduleEntry.entity.getProperty("state").setValue(STATE_NO_JOBFACTORY);
                scheduleEntry.entity.getProperty("next-start").setValue(null);
                scheduleEntry.entity.getProperty("next-stop").setValue(null);
                addHistory(scheduleEntry, STATE_NO_JOBFACTORY, null);
            }
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + ", exception: " + e);
            try {
                scheduleEntry.entity.getProperty("state").setValue(STATE_JOB_EXCEPTION);
                scheduleEntry.entity.getProperty("next-start").setValue(null);
                scheduleEntry.entity.getProperty("next-stop").setValue(null);
                addHistory(scheduleEntry, STATE_JOB_EXCEPTION, e.getMessage());
            } catch (Exception e1) {
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedule: " + scheduleEntry + " done");
    }

    private void calcSchedules(String groupName, String jobName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedules, group: " + groupName + ", job: " + jobName + " ...");
        for (Iterator iter = schedules.entrySet().iterator(); iter.hasNext(); ) {
            Entry scheduleEntry = (Entry) ((Map.Entry) iter.next()).getValue();
            if (scheduleEntry.schedule.getJobGroup().equals(groupName) && scheduleEntry.schedule.getJobName().equals(jobName)) {
                scheduleEntry.lastStartTime = -1;
                calcSchedule(scheduleEntry);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/calcSchedules, group: " + groupName + ", job: " + jobName + " done");
    }

    private void stopSchedule(Entry scheduleEntry) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedule: " + scheduleEntry + " ...");
        if (scheduleEntry.job != null) {
            JobException jobException = null;
            try {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedule: " + scheduleEntry + " stopping job...");
                if (scheduleEntry.schedule.isLoggingEnabled())
                    ctx.logSwiftlet.logInformation(ctx.schedulerSwiftlet.getName(), toString() + "/stopping job: " + scheduleEntry.job);
                scheduleEntry.job.stop();
                try {
                    scheduleEntry.entity.getProperty("state").setValue(STATE_STOPPED);
                    scheduleEntry.entity.getProperty("next-start").setValue(null);
                    scheduleEntry.entity.getProperty("next-stop").setValue(null);
                    addHistory(scheduleEntry, STATE_STOPPED, null);
                } catch (Exception e1) {
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedule: " + scheduleEntry + " stopping job done");
            } catch (JobException e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedule: " + scheduleEntry + " stopping job, exception: " + e);
                if (scheduleEntry.schedule.isLoggingEnabled())
                    ctx.logSwiftlet.logError(ctx.schedulerSwiftlet.getName(), toString() + "/stopping job, exception: " + e);
                jobException = e;
                try {
                    scheduleEntry.entity.getProperty("state").setValue(STATE_JOB_EXCEPTION);
                    scheduleEntry.entity.getProperty("next-start").setValue(null);
                    scheduleEntry.entity.getProperty("next-stop").setValue(null);
                    addHistory(scheduleEntry, STATE_JOB_EXCEPTION, e.getMessage());
                } catch (Exception e1) {
                }
            }
            JobFactory factory = getJobFactory(scheduleEntry);
            if (factory != null)
                factory.finished(scheduleEntry.job, jobException);
        }
        if (scheduleEntry.jobStart != null)
            ctx.timerSwiftlet.removeTimerListener(scheduleEntry.jobStart);
        if (scheduleEntry.jobStop != null)
            ctx.timerSwiftlet.removeTimerListener(scheduleEntry.jobStop);
        scheduleEntry.job = null;
        scheduleEntry.jobStart = null;
        scheduleEntry.jobStop = null;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedule: " + scheduleEntry + " done");
    }

    private void stopSchedules(String groupName, String jobName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedules, group: " + groupName + ", job: " + jobName + " ...");
        for (Iterator iter = schedules.entrySet().iterator(); iter.hasNext(); ) {
            Entry scheduleEntry = (Entry) ((Map.Entry) iter.next()).getValue();
            if (scheduleEntry.schedule.getJobGroup().equals(groupName) && scheduleEntry.schedule.getJobName().equals(jobName))
                stopSchedule(scheduleEntry);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedules, group: " + groupName + ", job: " + jobName + " done");
    }

    private void stopSchedules() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedules ...");
        for (Iterator iter = schedules.entrySet().iterator(); iter.hasNext(); ) {
            Entry scheduleEntry = (Entry) ((Map.Entry) iter.next()).getValue();
            stopSchedule(scheduleEntry);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stopSchedules done");
    }

    private Properties getParameters(Entry entry, JobFactory jobFactory) throws Exception {
        Properties prop = new Properties();
        Map jobParms = jobFactory.getJobParameters();
        Map actParms = entry.schedule.getParameters();
        if (jobParms != null) {
            for (Iterator iter = jobParms.entrySet().iterator(); iter.hasNext(); ) {
                JobParameter jp = (JobParameter) ((Map.Entry) iter.next()).getValue();
                String value = (String) actParms.get(jp.getName());
                if (value != null) {
                    JobParameterVerifier verifier = jp.getVerifier();
                    if (verifier != null)
                        verifier.verify(jp, value);
                    prop.setProperty(jp.getName(), value);
                } else if (jp.isMandatory())
                    throw new Exception("Missing mandatory Parameter: " + jp.getName());
            }
        }
        return prop;
    }

    private void addHistory(Entry entry, String state, String exception) {
        try {
            EntityList histList = (EntityList) entry.entity.getEntity("history");
            if (entry.histNo == 999) {
                entry.histNo = 0;
                Map map = histList.getEntities();
                if (map != null) {
                    for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                        histList.removeEntity((Entity) ((Map.Entry) iter.next()).getValue());
                    }
                }
            }
            String[] names = histList.getEntityNames();
            if (names != null && names.length == 20) {
                String min = null;
                for (int i = 0; i < names.length; i++) {
                    if (min == null || min.compareTo(names[i]) > 0)
                        min = names[i];
                }
                histList.removeEntity(histList.getEntity(min));
            }
            Entity entity = histList.createEntity();
            entity.setName(dfmt.format(++entry.histNo));
            entity.createCommands();
            entity.getProperty("logtime").setValue(fmt.format(new Date()));
            entity.getProperty("state").setValue(state);
            if (exception != null)
                entity.getProperty("xception").setValue(exception);
            histList.addEntity(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void visit(CalendarAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        calendars.put(po.getCalendar().getName(), po.getCalendar());
        List list = getSchedules(po.getCalendar().getName());
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Entry entry = (Entry) list.get(i);
                entry.lastStartTime = -1;
                calcSchedule(entry);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(CalendarRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        SchedulerCalendar cal = (SchedulerCalendar) calendars.remove(po.getName());
        if (cal != null) {
            List list = getSchedules(cal.getName());
            if (list != null) {
                for (int i = 0; i < list.size(); i++) {
                    Entry entry = (Entry) list.get(i);
                    entry.lastStartTime = -1;
                    calcSchedule(entry);
                }
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(CalendarChanged po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        SchedulerCalendar cal = po.getCalendar();
        calendars.put(cal.getName(), cal);
        List list = getSchedules(cal.getName());
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Entry entry = (Entry) list.get(i);
                entry.lastStartTime = -1;
                calcSchedule(entry);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ScheduleAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Schedule schedule = po.getSchedule();
        Entity entity = null;
        if (po.isMessageSchedule()) {
            entity = ctx.activeMessageScheduleList.createEntity();
            entity.setName(schedule.getName());
            entity.createCommands();
            entity.setDynamic(true);
            try {
                entity.getProperty("state").setValue(STATE_NONE);
                entity.getProperty("schedule-date-from").setValue(schedule.getDateFrom() == null ? ScheduleFactory.NOW : schedule.getDateFrom());
                entity.getProperty("schedule-date-to").setValue(schedule.getDateTo() == null ? ScheduleFactory.FOREVER : schedule.getDateTo());
                entity.getProperty("schedule-calendar").setValue(schedule.getCalendar());
                entity.getProperty("schedule-logging-enabled").setValue(new Boolean(schedule.isLoggingEnabled()));
                entity.getProperty("schedule-time-expression").setValue(schedule.getTimeExpression());
                ctx.activeMessageScheduleList.addEntity(entity);
            } catch (Exception e) {
            }
        } else {
            entity = ctx.activeScheduleList.createEntity();
            entity.setName(schedule.getName());
            entity.createCommands();
            entity.setDynamic(true);
            try {
                entity.getProperty("job-group").setValue(schedule.getJobGroup());
                entity.getProperty("job-name").setValue(schedule.getJobName());
                entity.getProperty("state").setValue(STATE_NONE);
                ctx.activeScheduleList.addEntity(entity);
            } catch (Exception e) {
            }
        }
        Entry scheduleEntry = new Entry(schedule, entity);
        scheduleEntry.messageSchedule = po.isMessageSchedule();
        addHistory(scheduleEntry, STATE_NONE, null);
        schedules.put(schedule.getName(), scheduleEntry);
        calcSchedule(scheduleEntry);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ScheduleRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Entry scheduleEntry = (Entry) schedules.remove(po.getName());
        if (scheduleEntry != null) {
            po.setSuccess(true);
            stopSchedule(scheduleEntry);
            try {
                if (po.isMessageSchedule()) {
                    scheduleEntry.entity.setState(DONOTHING);
                    ctx.activeMessageScheduleList.removeEntity(scheduleEntry.entity);
                } else
                    ctx.activeScheduleList.removeEntity(scheduleEntry.entity);
            } catch (EntityRemoveException e) {
            }
        } else
            po.setSuccess(false);
        Semaphore sem = po.getSemaphore();
        if (sem != null)
            sem.notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(ScheduleChanged po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Schedule schedule = po.getSchedule();
        Entry scheduleEntry = (Entry) schedules.get(schedule.getName());
        scheduleEntry.schedule = schedule;
        scheduleEntry.lastStartTime = -1;
        schedules.put(schedule.getName(), scheduleEntry);
        calcSchedule(scheduleEntry);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(JobFactoryAdded po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Map factories = (Map) jobGroups.get(po.getGroupName());
        if (factories == null) {
            factories = new HashMap();
            jobGroups.put(po.getGroupName(), factories);
        }
        factories.put(po.getName(), po.getFactory());
        calcSchedules(po.getGroupName(), po.getName());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(JobFactoryRemoved po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        stopSchedules(po.getGroupName(), po.getName());
        Map factories = (Map) jobGroups.get(po.getGroupName());
        if (factories != null) {
            factories.remove(po.getName());
            if (factories.size() == 0)
                jobGroups.remove(po.getGroupName());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(JobStart po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Entry entry = (Entry) schedules.get(po.getName());
        if (entry != null) {
            if (entry.jobStart == po) {
                entry.jobStart = null;
                JobFactory jobFactory = getJobFactory(entry);
                if (jobFactory != null) {
                    try {
                        Properties prop = getParameters(entry, jobFactory);
                        entry.job = jobFactory.getJobInstance();
                        if (po.getMaxRuntime() > 0) {
                            entry.jobStop = new JobStop(po.getName());
                            entry.jobStop.setScheduler(this);
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " registering jobStop: " + entry.jobStop);
                            try {
                                entry.entity.getProperty("next-stop").setValue(fmt.format(new Date(po.getStartTime() + po.getMaxRuntime())));
                            } catch (Exception e) {
                            }
                            ctx.timerSwiftlet.addInstantTimerListener(po.getMaxRuntime(), entry.jobStop, !entry.schedule.isApplySystemTimeChange());
                        }
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " dispatching job: " + entry.job);
                        runnerPool.dispatchTask(new JobRunner(po.getName(), entry.job, prop, entry, entry));
                    } catch (Exception e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + ", exception: " + e);
                        try {
                            entry.entity.getProperty("state").setValue(STATE_JOB_EXCEPTION);
                            entry.entity.getProperty("next-start").setValue(null);
                            entry.entity.getProperty("next-stop").setValue(null);
                            addHistory(entry, STATE_JOB_EXCEPTION, e.getMessage());
                        } catch (Exception e1) {
                        }
                    }
                } else {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " no job Factory!");
                }
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " invalid in the meantime!");
            }
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " removed in the meantime!");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(JobStop po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Entry entry = (Entry) schedules.get(po.getName());
        if (entry != null) {
            if (entry.jobStop == po) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " stopping job: " + entry.job);
                entry.jobStop = null;
                JobException exception = null;
                try {
                    entry.job.stop();
                } catch (JobException e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " stopping job, exception: " + e);
                    if (entry.schedule.isLoggingEnabled())
                        ctx.logSwiftlet.logError(ctx.schedulerSwiftlet.getName(), toString() + "/stopping job, exception: " + e);
                    exception = e;
                }
                pipelineQueue.enqueue(new JobTerminated(po.getName(), exception));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " rescheduled in the meantime!");
            }
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " removed in the meantime!");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(JobTerminated po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        Entry entry = (Entry) schedules.get(po.getName());
        if (entry != null) {
            try {
                entry.entity.getProperty("state").setValue(STATE_STOPPED);
                addHistory(entry, STATE_STOPPED, po.getJobException() != null ? po.getJobException().getMessage() : po.getMessage());
            } catch (Exception e1) {
            }
            if (entry.schedule.isLoggingEnabled())
                ctx.logSwiftlet.logInformation(ctx.schedulerSwiftlet.getName(), toString() + "/job has terminated: " + entry.job + ", message=" + po.getMessage());
            if (po.getJobException() != null)
                ctx.logSwiftlet.logError(ctx.schedulerSwiftlet.getName(), toString() + "/job has terminated: " + entry.job + ", exception=" + po.getJobException());
            JobFactory factory = getJobFactory(entry);
            if (factory != null)
                factory.finished(entry.job, po.getJobException());
            if (entry.jobStart != null)
                ctx.timerSwiftlet.removeTimerListener(entry.jobStart);
            if (entry.jobStop != null)
                ctx.timerSwiftlet.removeTimerListener(entry.jobStop);
            entry.jobStart = null;
            entry.jobStop = null;
            entry.job = null;
            JobException exception = po.getJobException();
            if (exception == null || exception.isFurtherScheduleAllowed())
                calcSchedule(entry);
            else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " no further schedule allowed!");
                try {
                    entry.entity.getProperty("state").setValue(STATE_JOB_EXCEPTION);
                    entry.entity.getProperty("next-start").setValue(null);
                    entry.entity.getProperty("next-stop").setValue(null);
                    addHistory(entry, STATE_JOB_EXCEPTION, exception.getMessage());
                } catch (Exception e) {
                }
            }
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " removed in the meantime!");
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void visit(Close po) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " ...");
        stopSchedules();
        calendars.clear();
        schedules.clear();
        jobGroups.clear();
        po.getSemaphore().notifySingleWaiter();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/" + po + " done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/close ...");
        Semaphore sem = new Semaphore();
        pipelineQueue.enqueue(new Close(sem));
        sem.waitHere();
        pipelineQueue.close();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/close done");
    }

    public String toString() {
        return "Scheduler";
    }

    private class Entry implements JobTerminationListener {
        Schedule schedule = null;
        JobStart jobStart = null;
        Job job = null;
        JobStop jobStop = null;
        Entity entity = null;
        int histNo = 0;
        int lastStartTime = -1;
        boolean messageSchedule = false;

        public Entry(Schedule schedule, Entity entity) {
            this.schedule = schedule;
            this.entity = entity;
        }

        public void jobTerminated() {
            jobTerminated((String) null);
        }

        public void jobTerminated(String s) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/jobTerminated, message=" + s);
            pipelineQueue.enqueue(new JobTerminated(schedule.getName(), s));
        }

        public void jobTerminated(JobException e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/jobTerminated, exception: " + e);
            pipelineQueue.enqueue(new JobTerminated(schedule.getName(), e));
        }

        public String toString() {
            return "[Entry, schedule=" + schedule + "]";
        }
    }

    private class JobRunner implements AsyncTask {
        String name = null;
        Job job = null;
        Properties prop = null;
        JobTerminationListener jtl = null;
        Entry scheduleEntry = null;
        boolean valid = true;

        public JobRunner(String name, Job job, Properties prop, JobTerminationListener jtl, Entry scheduleEntry) {
            this.name = name;
            this.job = job;
            this.prop = prop;
            this.jtl = jtl;
            this.scheduleEntry = scheduleEntry;
        }

        public boolean isValid() {
            return valid;
        }

        public String getDispatchToken() {
            return TP_RUNNER;
        }

        public String getDescription() {
            return "JobRunner, name=" + name + ", job=" + job;
        }

        public void stop() {
            valid = false;
        }

        public void run() {
            if (!valid)
                return;
            try {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), getDescription() + "/starting job ...");
                if (scheduleEntry.schedule.isLoggingEnabled())
                    ctx.logSwiftlet.logInformation(ctx.schedulerSwiftlet.getName(), getDescription() + "/starting job ...");
                try {
                    scheduleEntry.entity.getProperty("state").setValue(STATE_RUNNING);
                    addHistory(scheduleEntry, STATE_RUNNING, null);
                } catch (Exception e) {
                }
                job.start(prop, jtl);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), getDescription() + "/starting job done");
            } catch (JobException e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), getDescription() + "/starting job, exception: " + e);
                if (scheduleEntry.schedule.isLoggingEnabled())
                    ctx.logSwiftlet.logError(ctx.schedulerSwiftlet.getName(), getDescription() + "/starting job, exception: " + e);
                valid = false;
                pipelineQueue.enqueue(new JobTerminated(name, e));
            }
        }
    }
}
