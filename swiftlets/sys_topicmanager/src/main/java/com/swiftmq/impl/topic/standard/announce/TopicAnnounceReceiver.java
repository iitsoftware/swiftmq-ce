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

package com.swiftmq.impl.topic.standard.announce;

import com.swiftmq.impl.topic.standard.TopicManagerContext;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.MessageProcessor;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.versioning.*;
import com.swiftmq.tools.versioning.event.VersionedListener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TopicAnnounceReceiver extends MessageProcessor
        implements VersionVisitor {
    // Thread-Names
    TopicManagerContext ctx = null;
    String topicQueue = null;
    QueueReceiver topicReceiver = null;
    final AtomicReference<QueuePullTransaction> topicTransaction = new AtomicReference<>();
    MessageEntry messageEntry = null;
    final AtomicBoolean closed = new AtomicBoolean(false);
    TopicInfoFactory factory = new TopicInfoFactory();
    TopicInfoConverter converter = new TopicInfoConverter();
    DataByteArrayInputStream dis = new DataByteArrayInputStream();
    TopicInfoProcessor topicInfoProcessor = new TopicInfoProcessor();
    VersionObjectFactory versionObjectFactory = new VersionObjectFactory();

    public TopicAnnounceReceiver(TopicManagerContext ctx, String topicQueue) throws Exception {
        this.ctx = ctx;
        this.topicQueue = topicQueue;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ startup ...");
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ checking if queue " + topicQueue + " exists ...");
        try {
            if (!ctx.queueManager.isQueueDefined(topicQueue))
                ctx.queueManager.createQueue(topicQueue, (ActiveLogin) null);
            topicReceiver = ctx.queueManager.createQueueReceiver(topicQueue, null, null);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ startup: starting topic listener ...");
        try {
            topicTransaction.set(topicReceiver.createTransaction(false));
            topicTransaction.get().registerMessageProcessor(this);
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        ctx.announceReceiver = this;
    }

    public void setClosed() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ setClosed ...");
        closed.set(true);
        try {
            topicReceiver.close();
        } catch (Exception ignored) {
        }
    }

    public boolean isValid() {
        return !closed.get();
    }

    public void processMessage(MessageEntry messageEntry) {
        if (!closed.get()) {
            this.messageEntry = messageEntry;
            ctx.threadpoolSwiftlet.runAsync(this);
        }
    }

    public void processException(Exception exception) {
        messageEntry = null;
    }

    public String getDispatchToken() {
        return "none";
    }

    public String getDescription() {
        return "sys$topic/TopicAnnounceReceiver";
    }

    public void stop() {
        setClosed();
    }

    public void run() {
        if (!closed.get()) {
            try {
                topicTransaction.get().commit();
                BytesMessageImpl msg = (BytesMessageImpl) messageEntry.getMessage();
                msg.reset();
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/run, new message: " + msg);
                byte[] b = new byte[(int) msg.getBodyLength()];
                msg.readBytes(b);
                dis.reset();
                dis.setBuffer(b);
                VersionObject vo = (VersionObject) versionObjectFactory.createDumpable(dis.readInt());
                vo.readContent(dis);
                vo.accept(this);
                topicTransaction.set(topicReceiver.createTransaction(false));
                topicTransaction.get().registerMessageProcessor(this);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ exception occurred: " + e + ", EXITING");
                try {
                    ctx.queueManager.purgeQueue(topicQueue);
                } catch (Exception ignored) {
                }
            }
        }
    }

    public void visit(VersionNotification notification) {
        ctx.announceSender.versionNoteReceived(notification);
    }

    public void visit(Versioned versioned) {
        topicInfoProcessor.process(versioned);
    }

    public String toString() {
        return "TopicAnnounceReceiver";
    }

    private class TopicInfoProcessor extends VersionedProcessor
            implements VersionedListener {
        public TopicInfoProcessor() {
            super(null, null, converter, factory, false);
            setListener(TopicInfoFactory.getSupportedVersions(), this);
        }

        public void onAccept(VersionedDumpable vd) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onAccept, vd= " + vd);
            TopicInfo topicInfo = (TopicInfo) vd.getDumpable();
            if (topicInfo.isCreationInfo())
                ctx.announceSender.announceSubscriptions(topicInfo);
            else {
                try {
                    ctx.topicManager.processTopicInfo(topicInfo);
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onAccept, vd= " + vd + ", exception=" + e);
                    ctx.logSwiftlet.logError(ctx.topicManager.getName(), toString() + "/onAccept, vd= " + vd + ", exception=" + e);
                }
            }
        }

        public void onException(Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onException, exception= " + e);
            ctx.logSwiftlet.logError(ctx.topicManager.getName(), toString() + "/onException, exception= " + e);
        }

        public String toString() {
            return TopicAnnounceReceiver.this.toString() + "/TopicInfoProcessor";
        }
    }
}
