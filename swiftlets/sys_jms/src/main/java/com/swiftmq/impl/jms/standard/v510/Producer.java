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

package com.swiftmq.impl.jms.standard.v510;

import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.queue.QueueTransaction;

public class Producer implements TransactionFactory {
    protected SessionContext ctx = null;
    protected QueueSender sender = null;
    protected QueuePushTransaction transaction = null;
    protected boolean markedForClose = false;

    protected Producer(SessionContext ctx) {
        this.ctx = ctx;
    }

    protected void setQueueSender(QueueSender sender) {
        this.sender = sender;
    }

    /**
     * @return
     */
    public QueueTransaction createTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/createTransaction");
        transaction = sender.createTransaction();
        return transaction;
    }

    public QueuePushTransaction getTransaction() {
        return transaction;
    }

    public void markForClose() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/markForClose");
        markedForClose = true;
    }

    public boolean isMarkedForClose() {
        return markedForClose;
    }

    public void close() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + "/close");
        transaction = null;
        sender.close();
    }
}

