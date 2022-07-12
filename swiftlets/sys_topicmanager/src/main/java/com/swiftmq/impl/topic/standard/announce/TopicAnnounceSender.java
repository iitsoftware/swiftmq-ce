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
import com.swiftmq.impl.topic.standard.TopicManagerImpl;
import com.swiftmq.impl.topic.standard.announce.po.*;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityWatchListener;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.routing.Route;
import com.swiftmq.swiftlet.routing.event.RoutingEvent;
import com.swiftmq.swiftlet.routing.event.RoutingListener;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.*;
import com.swiftmq.tools.versioning.event.VersionedListener;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TopicAnnounceSender
        implements POAnnounceSenderVisitor, RoutingListener, EntityWatchListener {
    static final String TP_TOPICANNOUNCER = "sys$topicmanager.topic.announcer";
    TopicManagerContext ctx = null;
    boolean closed = false;
    PipelineQueue pipelineQueue = null;
    HashMap rtmList = new HashMap();
    HashMap announceSubs = new HashMap();
    DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
    TopicInfoFactory factory = new TopicInfoFactory();
    TopicInfoConverter converter = new TopicInfoConverter();

    public TopicAnnounceSender(TopicManagerContext ctx) {
        this.ctx = ctx;
        pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_TOPICANNOUNCER), TP_TOPICANNOUNCER, this);
        pipelineQueue.stopQueue();
        ctx.activeSubscriberList.addEntityWatchListener(this);
        ctx.announceSender = this;
    }

    // --> RoutingListener
    public void destinationAdded(RoutingEvent event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationAdded: " + event.getDestination());
        pipelineQueue.enqueue(new PODestinationAdded(ctx.routingSwiftlet.getRoute(event.getDestination())));
    }

    public void destinationActivated(RoutingEvent event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationActivated: " + event.getDestination());
        pipelineQueue.enqueue(new PODestinationActivated(event.getDestination()));
        pipelineQueue.enqueue(new POVersionNoteToSend(event.getDestination()));
    }

    public void destinationDeactivated(RoutingEvent event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationDeactivated: " + event.getDestination());
        pipelineQueue.enqueue(new PODestinationDeactivated(event.getDestination()));
    }

    public void destinationRemoved(RoutingEvent event) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationRemoved: " + event.getDestination());
        pipelineQueue.enqueue(new PODestinationRemoved(event.getDestination()));
    }
    // <-- RoutingListener

    // --> EntityWatchListener
    public void entityAdded(Entity parent, Entity newEntity) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/entityAdded: " + newEntity);
        Property prop = newEntity.getProperty("topic");
        String[] tokenized = ctx.topicManager.tokenizeTopicName((String) prop.getValue());
        pipelineQueue.enqueue(new POSubscriptionAdded(tokenized[0]));
    }

    public void entityRemoved(Entity parent, Entity delEntity) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/entityRemoved: " + delEntity);
        Property prop = delEntity.getProperty("topic");
        String[] tokenized = ctx.topicManager.tokenizeTopicName((String) prop.getValue());
        pipelineQueue.enqueue(new POSubscriptionRemoved(tokenized[0]));
    }
    // <-- EntityWatchListener

    // --> Exposed Methods
    public void start() {
        pipelineQueue.startQueue();
    }

    public void versionNoteReceived(VersionNotification vn) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/versionNoteReceived: " + vn);
        pipelineQueue.enqueue(new POVersionNoteReceived(vn));
    }

    public void destinationAdded(Route route) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationAdded: " + route.getDestination());
        pipelineQueue.enqueue(new PODestinationAdded(route));
        if (route.isActive()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/destinationAdded, activate: " + route.getDestination());
            pipelineQueue.enqueue(new PODestinationActivated(route.getDestination()));
            pipelineQueue.enqueue(new POVersionNoteToSend(route.getDestination()));
        }
    }

    public void routerRemoved(String routername) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/topicCreated: " + routername);
        pipelineQueue.enqueue(new PODestinationRemoved(routername));
    }

    public void topicCreated(String topicName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/topicCreated: " + topicName);
        pipelineQueue.enqueue(new POTopicCreated(topicName));
    }

    public void topicRemoved(String topicName) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/topicRemoved: " + topicName);
        pipelineQueue.enqueue(new POTopicRemoved(topicName));
    }

    public void announceSubscriptions(TopicInfo remoteTI) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/announceSubscriptions: " + remoteTI);
        pipelineQueue.enqueue(new POAnnounceSubscriptions(remoteTI));
    }
    // <-- Exposed Methods


    private void send(String dest, QueueSender sender, VersionObject vo) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/send, dest: " + dest + ", vo: " + vo);
        dos.rewind();
        dos.writeInt(vo.getDumpId());
        vo.writeContent(dos);
        BytesMessageImpl msg = new BytesMessageImpl();
        msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
        msg.setJMSPriority(MessageImpl.MAX_PRIORITY);
        msg.setJMSDestination(new QueueImpl(TopicManagerImpl.TOPIC_QUEUE + "@" + dest));
        QueuePushTransaction transaction = sender.createTransaction();
        transaction.putMessage(msg);
        transaction.commit();
    }

    private Versioned toVersioned(VersionedDumpable vd) throws Exception {
        dos.rewind();
        dos.writeInt(vd.getDumpable().getDumpId());
        vd.getDumpable().writeContent(dos);
        return new Versioned(vd.getVersion(), dos.getBuffer(), dos.getCount());
    }

    // --> Visitor methods
    public void visit(POVersionNoteToSend po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.get(po.getDestination());
        if (rtm != null) {
            try {
                VersionNotification vn = new VersionNotification(SwiftletManager.getInstance().getRouterName(), TopicInfoFactory.getSupportedVersions());
                send(po.getDestination(), rtm.getSender(), vn);
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", Exception: " + e);
            }
        }
    }

    public void visit(POVersionNoteReceived po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.get(po.getVersionNotification().getIdentifier());
        if (rtm == null) {
            rtm = new RemoteTopicManager(po.getVersionNotification().getIdentifier(), null);
            rtmList.put(po.getVersionNotification().getIdentifier(), rtm);
        }
        rtm.setListener(po.getVersionNotification().getAcceptedVersions(), rtm);
        if (rtm.getSender() != null)
            rtm.processBuffer();
        if (!rtm.isValid()) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM invalid after processBuffer, removing!");
            rtm.close();
            rtmList.remove(po.getVersionNotification().getIdentifier());
        }
    }

    public void visit(PODestinationAdded po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.get(po.getRoute().getDestination());
        if (rtm == null) {
            try {
                QueueSender sender = ctx.queueManager.createQueueSender(po.getRoute().getOutboundQueueName(), null);
                rtmList.put(po.getRoute().getDestination(), new RemoteTopicManager(po.getRoute().getDestination(), sender));
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", Exception: " + e);
            }
        } else {
            if (rtm.getSender() == null) {
                try {
                    QueueSender sender = ctx.queueManager.createQueueSender(po.getRoute().getOutboundQueueName(), null);
                    rtm.setSender(sender);
                    rtm.processBuffer();
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        rtmList.remove(po.getRoute().getDestination());
                    }
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", Exception: " + e);
                }
            }
        }
    }

    public void visit(PODestinationRemoved po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.remove(po.getDestination());
        if (rtm != null) {
            rtm.close();
        }
        ctx.topicManager.removeRemoteSubscriptions(po.getDestination());
    }

    public void visit(PODestinationActivated po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/ " + po);
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.get(po.getDestination());
        if (rtm != null) {
            try {
                for (Iterator iter = announceSubs.entrySet().iterator(); iter.hasNext(); ) {
                    AnnounceSubscription as = (AnnounceSubscription) ((Map.Entry) iter.next()).getValue();
                    // Send create info
                    VersionedDumpable vd = TopicInfoFactory.createTopicInfo(po.getDestination(), SwiftletManager.getInstance().getRouterName(), as.topicName, ctx.topicManager.tokenizeTopicName(as.topicName), true);
                    rtm.process(toVersioned(vd));
                    // Evtl. send subscription info
                    if (as.cnt > 0) {
                        vd = TopicInfoFactory.createTopicInfo(po.getDestination(), SwiftletManager.getInstance().getRouterName(), as.topicName, ctx.topicManager.tokenizeTopicName(as.topicName), as.cnt);
                        rtm.process(vd);
                    }
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        rtmList.remove(po.getDestination());
                    }
                }
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                if (!rtm.isValid()) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                    rtm.close();
                    rtmList.remove(po.getDestination());
                }
            }
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", no rtm object found!");
    }

    public void visit(PODestinationDeactivated po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
    }

    public void visit(POTopicCreated po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        String[] tt = ctx.topicManager.tokenizeTopicName(po.getTopicName());
        AnnounceSubscription as = (AnnounceSubscription) announceSubs.get(tt[0]);
        if (as == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", announce all (createInfo)");
            announceSubs.put(tt[0], new AnnounceSubscription(tt[0], 0));
            // AnnounceAll: CREATIONINFO!
            for (Iterator iter = rtmList.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                String dest = (String) entry.getKey();
                RemoteTopicManager rtm = (RemoteTopicManager) entry.getValue();
                VersionedDumpable vd = TopicInfoFactory.createTopicInfo(dest, SwiftletManager.getInstance().getRouterName(), tt[0], tt, true);
                try {
                    rtm.process(vd);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        iter.remove();
                    }
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        iter.remove();
                    }
                }
            }
        }
    }

    public void visit(POTopicRemoved po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        String[] tt = ctx.topicManager.tokenizeTopicName(po.getTopicName());
        AnnounceSubscription as = (AnnounceSubscription) announceSubs.get(tt[0]);
        if (as != null) {
            as.cnt = Math.max(0, as.cnt - 1);
            if (as.cnt == 0) {
                // Announce all
                for (Iterator iter = rtmList.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String dest = (String) entry.getKey();
                    RemoteTopicManager rtm = (RemoteTopicManager) entry.getValue();
                    VersionedDumpable vd = TopicInfoFactory.createTopicInfo(dest, SwiftletManager.getInstance().getRouterName(), tt[0], tt, as.cnt);
                    try {
                        rtm.process(vd);
                        if (!rtm.isValid()) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                            rtm.close();
                            iter.remove();
                        }
                    } catch (Exception e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                        if (!rtm.isValid()) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                            rtm.close();
                            iter.remove();
                        }
                    }
                }
            }
        }
    }

    public void visit(POSubscriptionAdded po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        String[] tt = ctx.topicManager.tokenizeTopicName(po.getTopicName());
        AnnounceSubscription as = (AnnounceSubscription) announceSubs.get(tt[0]);
        if (as == null) {
            as = new AnnounceSubscription(tt[0], 0);
            announceSubs.put(tt[0], as);
        }
        as.cnt++;
        if (as.cnt == 1) {
            // Announce all
            for (Iterator iter = rtmList.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry entry = (Map.Entry) iter.next();
                String dest = (String) entry.getKey();
                RemoteTopicManager rtm = (RemoteTopicManager) entry.getValue();
                VersionedDumpable vd = TopicInfoFactory.createTopicInfo(dest, SwiftletManager.getInstance().getRouterName(), tt[0], tt, as.cnt);
                try {
                    rtm.process(vd);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        iter.remove();
                    }
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        iter.remove();
                    }
                }
            }
        }
    }

    public void visit(POSubscriptionRemoved po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        String[] tt = ctx.topicManager.tokenizeTopicName(po.getTopicName());
        AnnounceSubscription as = (AnnounceSubscription) announceSubs.get(tt[0]);
        if (as != null) {
            as.cnt = Math.max(0, as.cnt - 1);
            if (as.cnt == 0) {
                // Announce all
                for (Iterator iter = rtmList.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String dest = (String) entry.getKey();
                    RemoteTopicManager rtm = (RemoteTopicManager) entry.getValue();
                    VersionedDumpable vd = TopicInfoFactory.createTopicInfo(dest, SwiftletManager.getInstance().getRouterName(), tt[0], tt, as.cnt);
                    try {
                        rtm.process(vd);
                        if (!rtm.isValid()) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                            rtm.close();
                            iter.remove();
                        }
                    } catch (Exception e) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                        if (!rtm.isValid()) {
                            if (ctx.traceSpace.enabled)
                                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                            rtm.close();
                            iter.remove();
                        }
                    }
                }
            }
        }
    }

    public void visit(POAnnounceSubscriptions po) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po);
        TopicInfo remoteTI = po.getTopicInfo();
        RemoteTopicManager rtm = (RemoteTopicManager) rtmList.get(remoteTI.getRouterName());
        if (rtm != null) {
            AnnounceSubscription as = (AnnounceSubscription) announceSubs.get(remoteTI.getTopicName());
            if (as != null && as.cnt > 0) {
                VersionedDumpable vd = TopicInfoFactory.createTopicInfo(remoteTI.getRouterName(), SwiftletManager.getInstance().getRouterName(), as.topicName, ctx.topicManager.tokenizeTopicName(as.topicName), as.cnt);
                try {
                    rtm.process(vd);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        rtmList.remove(remoteTI.getRouterName());
                    }
                } catch (Exception e) {
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", exception: " + e);
                    if (!rtm.isValid()) {
                        if (ctx.traceSpace.enabled)
                            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + ", RTM object invalid, close!");
                        rtm.close();
                        rtmList.remove(remoteTI.getRouterName());
                    }
                }
            }
        } else if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/" + po + " no RTM object found!");
    }
    // <-- Visitor methods

    public synchronized void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/close...");
        pipelineQueue.close();
        ctx.activeSubscriberList.removeEntityWatchListener(this);
        for (Iterator iter = rtmList.entrySet().iterator(); iter.hasNext(); ) {
            RemoteTopicManager rtm = (RemoteTopicManager) ((Map.Entry) iter.next()).getValue();
            rtm.close();
        }
        rtmList.clear();
        announceSubs.clear();
        closed = true;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/close...done");
    }

    public String toString() {
        return "TopicAnnounceSender";
    }

    private class RemoteTopicManager extends VersionedProcessor
            implements VersionedListener {
        String destination = null;
        QueueSender sender = null;
        boolean valid = true;

        public RemoteTopicManager(String destination, QueueSender sender) {
            super(null, null, converter, factory, true);
            this.destination = destination;
            this.sender = sender;
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/created");
        }

        public QueueSender getSender() {
            return sender;
        }

        public void setSender(QueueSender sender) {
            this.sender = sender;
        }

        protected boolean isReady() {
            return sender != null;
        }

        public boolean isValid() {
            return valid;
        }

        public void onAccept(VersionedDumpable vd) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onAccept, vd=" + vd);
            try {
                send(destination, sender, toVersioned(vd));
            } catch (Exception e) {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onAccept, vd=" + vd + ", exception=" + e);
                valid = false;
            }
        }

        public void onException(Exception e) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/onException, exception=" + e);
            ctx.logSwiftlet.logError(ctx.topicManager.getName(), toString() + "/onException, exception=" + e);
        }

        public void close() {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.topicManager.getName(), toString() + "/created");
            valid = false;
            try {
                sender.close();
            } catch (Exception ignored) {
            }
        }

        public String toString() {
            return TopicAnnounceSender.this.toString() + "/[RemoteTopicManager, destination=" + destination + ", valid=" + valid + "]";
        }
    }

    private class AnnounceSubscription {
        String topicName = null;
        int cnt = 0;

        public AnnounceSubscription(String topicName, int cnt) {
            this.topicName = topicName;
            this.cnt = cnt;
        }
    }

}
