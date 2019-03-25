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

import com.swiftmq.impl.scheduler.standard.po.ScheduleAdded;
import com.swiftmq.impl.scheduler.standard.po.ScheduleRemoved;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.mgmt.EntityRemoveListener;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.Queue;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class RequestProcessor extends MessageProcessor {
    static final String PROP_SCHED_ERROR = "JMS_SWIFTMQ_SCHEDULER_ERROR";
    static final String PROP_SCHED_ERROR_TEXT = "JMS_SWIFTMQ_SCHEDULER_ERROR_TEXT";
    static final String ADD = "add";
    static final String REMOVE = "remove";
    static final DecimalFormat dfmt = new DecimalFormat("0000");
    static final String TP_PROCESSOR = "sys$scheduler.requestprocessor";
    static final String PROP_SCHED_COMMAND = "JMS_SWIFTMQ_SCHEDULER_COMMAND";
    SwiftletContext ctx = null;
    ThreadPool myTP = null;
    QueueReceiver receiver = null;
    QueuePullTransaction transaction = null;
    boolean closed = false;
    MessageEntry messageEntry = null;
    DataByteArrayOutputStream dbos = new DataByteArrayOutputStream();
    DataByteArrayInputStream dbis = new DataByteArrayInputStream();
    int cnt = 0;
    boolean removeInProgress = false;

    public RequestProcessor(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        myTP = ctx.threadpoolSwiftlet.getPool(TP_PROCESSOR);
        if (!ctx.queueManager.isQueueDefined(ctx.INTERNAL_QUEUE))
            ctx.queueManager.createQueue(ctx.INTERNAL_QUEUE, (ActiveLogin) null);
        initSchedules();
        if (!ctx.queueManager.isQueueDefined(ctx.REQUEST_QUEUE))
            ctx.queueManager.createQueue(ctx.REQUEST_QUEUE, (ActiveLogin) null);
        receiver = ctx.queueManager.createQueueReceiver(ctx.REQUEST_QUEUE, null, null);
        transaction = receiver.createTransaction(false);
        transaction.registerMessageProcessor(this);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/created");
    }

    private void initSchedules() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/initSchedules ...");
        // Initializes Schedules, stored in queue sys$schedule
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(ctx.INTERNAL_QUEUE, null, null);
        QueuePullTransaction t = receiver.createTransaction(false);
        List list = new ArrayList();
        MessageEntry entry = null;
        while ((entry = t.getMessage(0)) != null) {
            String id = entry.getMessage().getStringProperty(ctx.PROP_SCHED_ID);
            list.add(ScheduleFactory.createSchedule(ctx, entry.getMessage(), id, ctx.JOBGROUP, ctx.JOBNAME, ctx.PARM, id));
        }
        t.rollback();
        receiver.close();
        for (int i = 0; i < list.size(); i++) {
            Schedule schedule = (Schedule) list.get(i);
            if (schedule instanceof AtSchedule)
                ((AtSchedule) schedule).setFirstScheduleAfterRouterStart(true);
            ctx.scheduler.enqueue(new ScheduleAdded(schedule, true));
        }
        ctx.activeMessageScheduleList.setEntityRemoveListener(new EntityRemoveListener() {
            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (delEntity.getState() != null && delEntity.getState().equals(Scheduler.DONOTHING))
                    return;
                try {
                    removeSchedule(delEntity.getName());
                    ctx.scheduler.enqueue(new ScheduleRemoved(delEntity.getName()));
                    ctx.logSwiftlet.logInformation(ctx.schedulerSwiftlet.getName(), "Message Job '" + delEntity.getName() + "' administratively removed!");
                } catch (Exception e) {
                    throw new EntityRemoveException(e.getMessage());
                }
            }
        });
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/initSchedules done");
    }

    public boolean isValid() {
        return !closed;
    }

    public void processMessage(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
        myTP.dispatchTask(this);
    }

    public void processException(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/processException, exception=" + e);
    }

    public String getDispatchToken() {
        return TP_PROCESSOR;
    }

    public String getDescription() {
        return ctx.schedulerSwiftlet.getName() + "/" + toString();
    }

    public void stop() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stop ...");
        closed = true;
        try {
            transaction.rollback();
        } catch (Exception e) {
        }
        try {
            receiver.close();
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/stop done");
    }

    private String generateId() {
        String id = System.currentTimeMillis() + "-" + dfmt.format(++cnt);
        if (cnt == 9999)
            cnt = 0;
        return id;
    }


    private MessageImpl copyMessage(MessageImpl msg) throws Exception {
        dbos.rewind();
        msg.writeContent(dbos);
        dbis.reset();
        dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
        MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
        msgCopy.readContent(dbis);
        return msgCopy;
    }

    private void sendReply(MessageImpl msg) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/sendReply, msg=" + msg + " ...");
        Queue dest = (Queue) msg.getJMSReplyTo();
        if (dest == null)
            return;
        QueueSender sender = ctx.queueManager.createQueueSender(dest.getQueueName(), null);
        QueuePushTransaction t = sender.createTransaction();
        msg.setJMSDestination(dest);
        msg.setJMSReplyTo(null);
        t.putMessage(msg);
        t.commit();
        sender.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/sendReply, msg=" + msg + " done");
    }

    private void storeSchedule(MessageImpl msg) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/storeSchedule, msg=" + msg + " ...");
        QueueSender sender = ctx.queueManager.createQueueSender(ctx.INTERNAL_QUEUE, null);
        QueuePushTransaction t = sender.createTransaction();
        t.putMessage(msg);
        t.commit();
        sender.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/storeSchedule, msg=" + msg + " done");
    }

    private void removeSchedule(String id) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/removeSchedule, id=" + id + " ...");
        MessageSelector selector = new MessageSelector(ctx.PROP_SCHED_ID + " = '" + id + "'");
        selector.compile();
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(ctx.INTERNAL_QUEUE, null, selector);
        QueuePullTransaction t = receiver.createTransaction(false);
        MessageEntry entry = t.getMessage(0, selector);
        t.commit();
        receiver.close();
        if (entry == null)
            throw new Exception("Schedule with ID '" + id + "' not found!");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/removeSchedule, id=" + id + " done");
    }

    public void run() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/run, messageEntry=" + messageEntry + " ...");
        try {
            MessageImpl msg = messageEntry.getMessage();
            boolean error = true;
            String errorText = null;
            String cmd = msg.getStringProperty(PROP_SCHED_COMMAND);
            String id = null;
            if (cmd != null) {
                cmd = cmd.toLowerCase();
                if (cmd.equals(ADD)) {
                    // Add a Schedule
                    try {
                        // Create Schedule
                        id = generateId();
                        Schedule schedule = ScheduleFactory.createSchedule(ctx, msg, id, ctx.JOBGROUP, ctx.JOBNAME, ctx.PARM, id);
                        // Check destination and type
                        String dest = msg.getStringProperty(ctx.PROP_SCHED_DEST);
                        if (dest == null)
                            throw new Exception("Missing Property: " + ctx.PROP_SCHED_DEST);
                        String destType = msg.getStringProperty(ctx.PROP_SCHED_DEST_TYPE);
                        if (destType == null)
                            throw new Exception("Missing Property: " + ctx.PROP_SCHED_DEST_TYPE);
                        destType = destType.toLowerCase();
                        if (!(destType.equals(ctx.QUEUE) || destType.equals(ctx.TOPIC)))
                            throw new Exception("Property " + ctx.PROP_SCHED_DEST_TYPE + " must be of value '" + ctx.QUEUE + "' or '" + ctx.TOPIC + "'");
                        // Store Message in sys$schedule
                        MessageImpl msg1 = copyMessage(msg);
                        msg1.setStringProperty(ctx.PROP_SCHED_ID, id);
                        storeSchedule(msg1);
                        // Add Schedule to Scheduler
                        ctx.scheduler.enqueue(new ScheduleAdded(schedule, true));
                        error = false;
                    } catch (Exception e) {
                        // Error creating Schedule
                        errorText = "Error creating Schedule: " + e;
                    }
                } else if (cmd.equals(REMOVE)) {
                    // Remove a Schedule
                    id = msg.getStringProperty(ctx.PROP_SCHED_ID);
                    if (id == null) {
                        errorText = "Missing Property " + ctx.PROP_SCHED_ID;
                    } else {
                        try {
                            removeSchedule(id);
                            ctx.scheduler.enqueue(new ScheduleRemoved(id, true));
                            error = false;
                        } catch (Exception e) {
                            errorText = e.getMessage();
                        }
                    }
                } else {
                    // Invalid Command
                    errorText = "Invalid command '" + cmd + "' in Property " + PROP_SCHED_COMMAND + ", expected '" + ADD + "' or '" + REMOVE + "'";
                }
            } else {
                // no command given
                errorText = "No command given in Property " + PROP_SCHED_COMMAND + ", expected '" + ADD + "' or '" + REMOVE + "'";
            }
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/run, id=" + id + ", error=" + error + ", errorText=" + errorText);
            MessageImpl msgCopy = copyMessage(msg);
            if (id != null)
                msgCopy.setStringProperty(ctx.PROP_SCHED_ID, id);
            msgCopy.setBooleanProperty(PROP_SCHED_ERROR, error);
            if (error)
                msgCopy.setStringProperty(PROP_SCHED_ERROR_TEXT, errorText);
            sendReply(msgCopy);
        } catch (Exception e) {
        }
        try {
            transaction.commit();
            transaction = receiver.createTransaction(false);
            transaction.registerMessageProcessor(this);
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.schedulerSwiftlet.getName(), toString() + "/run, messageEntry=" + messageEntry + " done");
    }

    public String toString() {
        return "RequestProcessor";
    }
}
