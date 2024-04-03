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

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.swiftlet.queue.*;

public class Consumer implements TransactionFactory {
    protected SessionContext ctx = null;
    protected QueueReceiver receiver = null;
    protected Selector selector = null;
    protected QueuePullTransaction readTransaction = null;
    protected QueuePullTransaction transaction = null;
    protected int clientDispatchId = -1;
    protected int clientListenerId = -1;
    protected MessageProcessor messageProcessor = null;
    protected boolean hasListener = false;
    protected boolean markedForClose = false;

    protected Consumer(SessionContext ctx) {
        this.ctx = ctx;
    }

    protected boolean isAutoCommit() {
        return false;
    }

    protected void setQueueReceiver(QueueReceiver receiver) {
        this.receiver = receiver;
    }

    protected void setSelector(Selector selector) {
        this.selector = selector;
    }


    public Selector getSelector() {
        return selector;
    }

    public QueueTransaction createTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/createTransaction");
        transaction = receiver.createTransaction(true);
        return transaction;
    }

    public QueueTransaction createReadTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/createReadTransaction");
        readTransaction = receiver.createTransaction(false);
        return readTransaction;
    }

    public QueuePullTransaction getTransaction() {
        return transaction;
    }

    public QueuePullTransaction getReadTransaction() {
        return readTransaction;
    }

    public QueueTransaction createDuplicateTransaction() throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/createDuplicateTransaction");
        return receiver.createTransaction(false);
    }

    public void setMessageListener(int clientDispatchId, int clientListenerId, MessageProcessor messageProcessor) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/setMessageListener, clientDispatchId=" + clientDispatchId + ", clientListenerId=" + clientListenerId);
        this.clientDispatchId = clientDispatchId;
        this.clientListenerId = clientListenerId;
        this.messageProcessor = messageProcessor;
        hasListener = true;
    }

    public int getClientDispatchId() {
        return clientDispatchId;
    }

    public int getClientListenerId() {
        return clientListenerId;
    }

    public MessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    public void removeMessageListener() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/removeMessageListener");
        this.clientDispatchId = -1;
        this.clientListenerId = -1;
        this.messageProcessor = null;
        hasListener = false;
    }

    public boolean hasListener() {
        return hasListener;
    }

    public void markForClose() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/markForClose");
        try {
            if (readTransaction != null && !readTransaction.isClosed()) {
                if (messageProcessor != null)
                    readTransaction.unregisterMessageProcessor(messageProcessor);
            }
        } catch (Exception ignored) {
        }
        messageProcessor = null;
        markedForClose = true;
    }

    public boolean isMarkedForClose() {
        return markedForClose;
    }

    protected void close() throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + this + "/close");
        if (readTransaction != null && !readTransaction.isClosed()) {
            if (messageProcessor != null)
                readTransaction.unregisterMessageProcessor(messageProcessor);
            readTransaction.rollback();
            readTransaction = null;
        }
        messageProcessor = null;
        transaction = null;
        readTransaction = null;
        receiver.close();
    }
}

