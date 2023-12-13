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

import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.smqp.v750.*;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.Property;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.tools.collection.ConcurrentExpandableList;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import java.util.stream.IntStream;

public class BrowserManager {
    ConcurrentExpandableList<com.swiftmq.swiftlet.queue.QueueBrowser> queueBrowsers = new ConcurrentExpandableList<>();
    EntityList browserEntityList = null;
    SessionContext ctx = null;

    public BrowserManager(SessionContext ctx) {
        this.ctx = ctx;
        if (ctx.sessionEntity != null)
            browserEntityList = (EntityList) ctx.sessionEntity.getEntity("browser");
    }

    public void createBrowser(CreateBrowserRequest request) {
        CreateBrowserReply reply = (CreateBrowserReply) request.createReply();
        QueueImpl queue = request.getQueue();
        String messageSelector = request.getMessageSelector();
        MessageSelector msel = null;
        String queueName = null;
        try {
            queueName = queue.getQueueName();
        } catch (JMSException ignored) {
        }
        try {
            if (messageSelector != null) {
                msel = new MessageSelector(messageSelector);
                msel.compile();
            }
            if (!ctx.queueManager.isQueueRunning(queueName)) {
                ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": Invalid destination: " + queueName);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Invalid destination: " + queue);
                reply.setOk(false);
                reply.setException(new InvalidDestinationException("Invalid destination: " + queueName));
            } else {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Creating browser with selector: " + msel);
                com.swiftmq.swiftlet.queue.QueueBrowser queueBrowser = ctx.queueManager.createQueueBrowser(queueName, ctx.activeLogin, msel);
                int idx = queueBrowsers.add(queueBrowser);
                reply.setOk(true);
                reply.setQueueBrowserId(idx);
                if (browserEntityList != null) {
                    Entity browserEntity = browserEntityList.createEntity();
                    browserEntity.setName(queueName + "-" + idx);
                    browserEntity.setDynamicObject(queueBrowser);
                    browserEntity.createCommands();
                    Property prop = browserEntity.getProperty("queue");
                    prop.setValue(queueName);
                    prop.setReadOnly(true);
                    prop = browserEntity.getProperty("selector");
                    if (msel != null) {
                        prop.setValue(msel.getConditionString());
                    }
                    prop.setReadOnly(true);
                    browserEntityList.addEntity(browserEntity);
                }
            }
        } catch (InvalidSelectorException e) {
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": CreateBrowser has invalid Selector: " + e);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": CreateBrowser has invalid Selector: " + e);
            reply.setOk(false);
            reply.setException(e);
        } catch (Exception e) {
            ctx.logSwiftlet.logWarning("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during createQueueBrowser: " + e);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during createQueueBrowser: " + e);
            reply.setOk(false);
            reply.setException(e);
        }
        reply.send();
    }

    public void fetchBrowserMessage(FetchBrowserMessageRequest request) {
        FetchBrowserMessageReply reply = (FetchBrowserMessageReply) request.createReply();
        int browserId = request.getQueueBrowserId();
        try {
            com.swiftmq.swiftlet.queue.QueueBrowser browser = queueBrowsers.get(browserId);
            if (request.isResetRequired())
                browser.resetBrowser();
            browser.setLastMessageIndex(request.getLastMessageIndex());
            MessageEntry me = browser.getNextMessage();
            reply.setOk(true);
            reply.setMessageEntry(me);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": get next message failed: " + e.getMessage());
            reply.setOk(false);
            reply.setException(e);
        }
        reply.send();
    }

    public void closeBrowser(CloseBrowserRequest request) {
        CloseBrowserReply reply = (CloseBrowserReply) request.createReply();
        try {
            int browserId = request.getQueueBrowserId();
            com.swiftmq.swiftlet.queue.QueueBrowser browser = (com.swiftmq.swiftlet.queue.QueueBrowser) queueBrowsers.get(browserId);
            if (!browser.isClosed())
                browser.close();
            queueBrowsers.remove(browserId);
            reply.setOk(true);
            if (browserEntityList != null)
                browserEntityList.removeDynamicEntity(browser);
        } catch (Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": Exception during close queue browser: " + e);
            reply.setOk(false);
            reply.setException(e);
        }
        reply.send();
    }

    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$jms", ctx.tracePrefix + "/" + toString() + ": closing browsers");
        IntStream.range(0, queueBrowsers.size()).mapToObj(i -> queueBrowsers.get(i)).filter(b -> b != null && !b.isClosed()).forEach(b -> {
            try {
                b.close();
            } catch (Exception ignored) {
            }
        });
        queueBrowsers.clear();
    }

    public String toString() {
        return "BrowserManager";
    }
}

