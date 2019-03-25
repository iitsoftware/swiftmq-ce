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

package com.swiftmq.impl.queue.standard.jobs;

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.scheduler.Job;
import com.swiftmq.swiftlet.scheduler.JobException;
import com.swiftmq.swiftlet.scheduler.JobTerminationListener;

import javax.jms.InvalidSelectorException;
import java.util.Properties;

public class QueueMoverJob implements Job {
    SwiftletContext ctx = null;
    boolean stopCalled = false;
    Properties properties = null;
    QueueReceiver receiver = null;
    QueuePullTransaction pullTx = null;
    QueueSender sender = null;
    QueuePushTransaction pushTx = null;
    QueueImpl targetQueue = null;

    public QueueMoverJob(SwiftletContext ctx) {
        this.ctx = ctx;
    }

    private void terminate() {
        try {
            if (pullTx != null)
                pullTx.rollback();
            pullTx = null;
        } catch (Exception e) {
        }
        try {
            if (receiver != null)
                receiver.close();
            receiver = null;
        } catch (Exception e) {
        }
        try {
            if (pushTx != null)
                pushTx.rollback();
            pushTx = null;
        } catch (Exception e) {
        }
        try {
            if (sender != null)
                sender.close();
            sender = null;
        } catch (Exception e) {
        }
    }

    public void start(Properties properties, JobTerminationListener jobTerminationListener) throws JobException {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " ...");
        this.properties = properties;
        int cnt = 0;
        String queueName = properties.getProperty("Source Queue");
        if (!queueName.startsWith("tpc$")) {
            MessageSelector selector = null;
            String s = properties.getProperty("Message Selector");
            if (s != null) {
                selector = new MessageSelector(s);
                try {
                    selector.compile();
                } catch (InvalidSelectorException e) {
                    throw new JobException(e.toString(), e, false);
                }
            }
            try {
                receiver = ctx.queueManager.createQueueReceiver(queueName, null, selector);
                pullTx = receiver.createTransaction(false);
                targetQueue = new QueueImpl(properties.getProperty("Target Queue"));
                sender = ctx.queueManager.createQueueSender(targetQueue.getQueueName(), null);
                pushTx = sender.createTransaction();
            } catch (Exception e) {
                throw new JobException(e.toString(), e, false);
            }
            if (stopCalled) {
                terminate();
                return;
            }
            try {
                MessageEntry entry = null;
                while ((entry = pullTx.getMessage(0, selector)) != null) {
                    MessageImpl msg = entry.getMessage();
                    msg.setJMSDestination(targetQueue);
                    msg.setSourceRouter(null);
                    msg.setDestRouter(null);
                    pushTx.putMessage(msg);
                    cnt++;
                    pushTx.commit();
                    pullTx.commit();
                    if (stopCalled) {
                        terminate();
                        return;
                    }
                    pullTx = receiver.createTransaction(false);
                    pushTx = sender.createTransaction();
                }
            } catch (Exception e) {
                terminate();
                throw new JobException(e.toString(), e, false);
            }
        }
        terminate();
        jobTerminationListener.jobTerminated(cnt + " Messages moved");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start, properties=" + properties + " done, cnt=" + cnt);
    }

    public void stop() throws JobException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop ...");
        stopCalled = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop done");
    }

    public String toString() {
        return "[QueueMoverJob, properties=" + properties + "]";
    }
}
