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

package com.swiftmq.impl.queue.standard.cluster;

import com.swiftmq.impl.queue.standard.QueueManagerImpl;
import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

public class Redispatcher extends MessageProcessor {
    SwiftletContext ctx = null;
    volatile String sourceQueueName = null;
    volatile String targetQueueName = null;
    volatile QueueReceiver receiver = null;
    volatile QueuePullTransaction pullTx = null;
    volatile QueueSender sender = null;
    volatile QueueImpl targetQueue = null;
    volatile AbstractQueue sourceQueue = null;
    volatile ThreadPool myTP = null;
    volatile boolean closed = false;
    volatile MessageImpl message = null;
    volatile int cnt = 0;
    volatile DispatchPolicy dispatchPolicy = null;

    public Redispatcher(SwiftletContext ctx, String sourceQueueName, String targetQueueName) throws Exception {
        this.ctx = ctx;
        this.sourceQueueName = sourceQueueName;
        this.targetQueueName = targetQueueName;
        dispatchPolicy = ctx.dispatchPolicyRegistry.get(targetQueueName);
        myTP = ctx.threadpoolSwiftlet.getPool(QueueManagerImpl.TP_REDISPATCHER);
        sourceQueue = ctx.queueManager.getQueueForInternalUse(sourceQueueName);
        receiver = ctx.queueManager.createQueueReceiver(sourceQueueName, null, null);
        sourceQueue.decReceiverCount();
        targetQueue = new QueueImpl(targetQueueName);
        sender = ctx.queueManager.createQueueSender(targetQueueName, null);
        pullTx = receiver.createTransaction(false);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/created");
    }

    public void start() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/start");
        pullTx.registerMessageProcessor(this);
    }

    public String getDescription() {
        return ctx.queueManager.getName() + "/Redispatcher";
    }

    public String getDispatchToken() {
        return QueueManagerImpl.TP_REDISPATCHER;
    }

    public boolean isValid() {
        return true;
    }

    public void processException(Exception exception) {
        if (closed)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/processException: " + exception + ", EXITING!");
    }

    public void processMessage(MessageEntry messageEntry) {
        message = messageEntry.getMessage();
        myTP.dispatchTask(this);
    }

    public void run() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/run ...");
        try {
            message.setJMSDestination(targetQueue);
            QueuePushTransaction pushTx = sender.createTransaction();
            pushTx.putMessage(message);
            pushTx.commit();
            pullTx.commit();
            cnt++;
            if (!closed && sourceQueue.getReceiverCount() == 0 && sourceQueue.getNumberQueueMessages() > 0 && dispatchPolicy.isReceiverSomewhere()) {
                pullTx = receiver.createTransaction(false);
                pullTx.registerMessageProcessor(this);
            } else
                stop();
        } catch (Exception e) {
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/run done");
    }

    public void stop() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/stop, cnt=" + cnt);
        closed = true;
        try {
            pullTx.rollback();
        } catch (Exception e) {
        }
        try {
            sourceQueue.incReceiverCount();
            receiver.close();
        } catch (Exception e) {
        }
        try {
            sender.close();
        } catch (Exception e) {
        }
        ctx.redispatcherController.redispatcherFinished(sourceQueueName);
    }

    public String toString() {
        return "Redispatcher, source=" + sourceQueueName + ", target=" + targetQueueName;
    }
}
