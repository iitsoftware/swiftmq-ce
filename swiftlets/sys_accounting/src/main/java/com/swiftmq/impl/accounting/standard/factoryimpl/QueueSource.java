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

package com.swiftmq.impl.accounting.standard.factoryimpl;

import com.swiftmq.impl.accounting.standard.SwiftletContext;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.accounting.AccountingSink;
import com.swiftmq.swiftlet.accounting.AccountingSource;
import com.swiftmq.swiftlet.accounting.StopListener;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.threadpool.ThreadPool;

public class QueueSource extends MessageProcessor implements AccountingSource {
    static final String TP_RUNNER = "sys$accounting.sourcerunner";

    SwiftletContext ctx = null;
    String queueName = null;
    volatile ThreadPool myTP = null;
    volatile QueueReceiver receiver = null;
    volatile QueuePullTransaction tx = null;
    volatile StopListener stopListener = null;
    volatile AccountingSink sink = null;
    volatile MessageEntry entry = null;
    volatile boolean stopped = false;

    public QueueSource(SwiftletContext ctx, String queueName, Selector selector) throws Exception {
        super(selector);
        this.ctx = ctx;
        this.queueName = queueName;
        myTP = ctx.threadpoolSwiftlet.getPool(TP_RUNNER);
        receiver = ctx.queueManager.createQueueReceiver(queueName, null, selector);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/created");
    }

    private void terminate(Exception e) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/terminate, exception=" + e);
        stopped = true;
        try {
            receiver.close();
            receiver = null;
        } catch (Exception e1) {
        }
        if (stopListener != null)
            stopListener.sourceStopped(this, e);
    }

    public void setStopListener(StopListener stopListener) {
        this.stopListener = stopListener;
    }

    public void startAccounting(AccountingSink sink) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/startAccounting, sink=" + sink);
        this.sink = sink;
        tx = receiver.createTransaction(false);
        tx.registerMessageProcessor(this);
    }

    public void stopAccounting() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/stopAccounting");
        stopListener = null;
        terminate(null);
    }

    public boolean isValid() {
        return !stopped;
    }

    public void processMessage(MessageEntry entry) {
        this.entry = entry;
        myTP.dispatchTask(this);
    }

    public void processException(Exception e) {
        terminate(e);
    }

    public String getDispatchToken() {
        return TP_RUNNER;
    }

    public String getDescription() {
        return ctx.accountingSwiftlet.getName() + "/" + toString();
    }

    public void stop() {
        terminate(null);
    }

    public void run() {
        MessageImpl msg = entry.getMessage();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.accountingSwiftlet.getName(), toString() + "/run, msg=" + msg);
        if (msg instanceof MapMessageImpl) {
            try {
                sink.add((MapMessageImpl) msg);
                tx.commit();
                tx = receiver.createTransaction(false);
                tx.registerMessageProcessor(this);
            } catch (Exception e) {
                terminate(e);
            }
        } else
            terminate(new Exception("Message is not instanceof MapMessageImpl!"));

    }

    public String toString() {
        return "QueueSource, queue=" + queueName;
    }
}
