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
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;

import java.util.List;

public class ClusterMetricPublisher implements TimerListener, PropertyChangeListener {
    static final String PROP_SOURCE_ROUTER = "sourcerouter";
    SwiftletContext ctx = null;
    QueueSender sender = null;
    DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
    Property prop = null;
    long interval = -1;

    public ClusterMetricPublisher(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        sender = ctx.queueManager.createQueueSender(ctx.topicManager.getQueueForTopic(QueueManagerImpl.CLUSTER_TOPIC), null);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/created");
        prop = ctx.root.getProperty("cluster-metric-interval");
        interval = ((Long) prop.getValue()).longValue();
        prop.setPropertyChangeListener(this);
        intervalChanged(-1, interval);
    }

    private void intervalChanged(long oldInterval, long newInterval) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/intervalChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/intervalChanged: removeTimerListener for interval " + oldInterval);
            ctx.timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/intervalChanged: addTimerListener for interval " + newInterval);
            ctx.timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    private Versioned createVersioned(int version, Dumpable dumpable) throws Exception {
        dos.rewind();
        Dumpalizer.dump(dos, dumpable);
        return new Versioned(version, dos.getBuffer(), dos.getCount());
    }

    private BytesMessageImpl createMessage(Versionable versionable) throws Exception {
        BytesMessageImpl msg = new BytesMessageImpl();
        versionable.transferToMessage(msg);
        msg.setJMSDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);
        msg.setJMSDestination(new TopicImpl(QueueManagerImpl.CLUSTER_TOPIC));
        msg.setJMSPriority(MessageImpl.MAX_PRIORITY);
        if (interval > 0)
            msg.setJMSExpiration(System.currentTimeMillis() + interval * 2);
        msg.setStringProperty(PROP_SOURCE_ROUTER, SwiftletManager.getInstance().getRouterName());
        msg.setReadOnly(false);
        return msg;
    }

    public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException {
        intervalChanged((Long) oldValue, (Long) newValue);
        interval = (Long) newValue;
    }

    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/performTimeAction ...");
        ClusteredQueueMetricCollection cmc = ctx.dispatchPolicyRegistry.getClusteredQueueMetricCollection();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/performTimeAction, cmc=" + cmc);
        List list = cmc.getClusteredQueueMetrics();
        if (list != null) {
            for (Object o : list) {
                ClusteredQueueMetric cm = (ClusteredQueueMetric) o;
                List list2 = cm.getQueueMetrics();
                if (list2 != null) {
                    for (Object object : list2) {
                        QueueMetric qm = (QueueMetric) object;
                        AbstractQueue queue = ctx.queueManager.getQueueForInternalUse(qm.getQueueName());
                        if (queue != null) {
                            try {
                                if (ctx.traceSpace.enabled)
                                    ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/performTimeAction, queue=" + qm.getQueueName() + ", qm.hasReceiver()=" + qm.hasReceiver() + ", cm.isReceiverSomewhere()=" + cm.isReceiverSomewhere() + ", qm.isRedispatch()=" + qm.isRedispatch() + ", queue.getNumberQueueMessages()=" + queue.getNumberQueueMessages());
                                if (!qm.hasReceiver() && cm.isReceiverSomewhere() && qm.isRedispatch() && queue.getNumberQueueMessages() > 0)
                                    ctx.redispatcherController.redispatch(qm.getQueueName(), cm.getClusteredQueueName());
                            } catch (QueueException e) {

                            }
                        }
                    }
                }
            }
        }
        try {
            QueuePushTransaction transaction = sender.createTransaction();
            Versionable versionable = new Versionable();
            versionable.addVersioned(700, createVersioned(700, cmc), "com.swiftmq.impl.queue.standard.cluster.v700.MetricFactory");
            BytesMessageImpl msg = createMessage(versionable);
            transaction.putMessage(msg);
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/performTimeAction, exception=" + e);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/performTimeAction done");
    }

    public void close() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.queueManager.getName(), toString() + "/close");
        intervalChanged(interval, -1);
        prop.setPropertyChangeListener(null);
        try {
            sender.close();
        } catch (QueueException e) {
        }
    }

    public String toString() {
        return "ClusterMetricPublisher";
    }
}
