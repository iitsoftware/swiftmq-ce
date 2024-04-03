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

import com.swiftmq.impl.queue.standard.SwiftletContext;
import com.swiftmq.impl.queue.standard.cluster.v700.ClusteredQueueMetricImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.AbstractQueue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RoundRobinDispatchPolicy implements DispatchPolicy {
    SwiftletContext ctx = null;
    String clusteredQueueName = null;
    List localMetrics = new LinkedList();
    final AtomicInteger nextLocal = new AtomicInteger(-1);
    List receiverMetrics = new LinkedList();
    final AtomicInteger nextReceiver = new AtomicInteger(-1);
    List noReceiverMetrics = new LinkedList();
    final AtomicInteger nextNoReceiver = new AtomicInteger(-1);
    final AtomicReference<DispatchPolicyListener> listener = new AtomicReference<>();
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public RoundRobinDispatchPolicy(SwiftletContext ctx, String clusteredQueueName) {
        this.ctx = ctx;
        this.clusteredQueueName = clusteredQueueName;
    }

    private void removeRouterMetrics(String routerName, List list) {
        for (Iterator iter = list.iterator(); iter.hasNext(); ) {
            QueueMetric qm = (QueueMetric) iter.next();
            if (qm.getRouterName() != null && qm.getRouterName().equals(routerName)) {
                iter.remove();
                DispatchPolicyListener listenerInstance = listener.get();
                if (listenerInstance != null)
                    listenerInstance.dispatchQueueRemoved(qm.getQueueName());
            }
        }
    }

    private QueueMetric getMetric(String queueName, List list) {
        for (Object o : list) {
            QueueMetric qm = (QueueMetric) o;
            if (qm.getQueueName().equals(queueName))
                return qm;
        }
        return null;
    }

    public void setDispatchPolicyListener(DispatchPolicyListener listener) {
        this.listener.set(listener);
    }

    public void receiverCountChanged(AbstractQueue abstractQueue, int receiverCount) {
        lock.writeLock().lock();
        try {
            if (receiverCount == 0 || receiverCount == 1) {
                QueueMetric qm = getMetric(abstractQueue.getQueueName(), localMetrics);
                if (qm != null) {
                    if (qm.hasReceiver())
                        receiverMetrics.remove(qm);
                    else
                        noReceiverMetrics.remove(qm);
                    qm.setHasReceiver(receiverCount == 1);
                    if (qm.hasReceiver())
                        receiverMetrics.add(qm);
                    else
                        noReceiverMetrics.add(qm);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void addLocalMetric(QueueMetric metric) {
        lock.writeLock().lock();
        try {
            localMetrics.add(metric);
            if (metric.hasReceiver())
                receiverMetrics.add(metric);
            else
                noReceiverMetrics.add(metric);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeLocalMetric(QueueMetric metric) {
        lock.writeLock().lock();
        try {
            localMetrics.remove(metric);
            if (metric.hasReceiver())
                receiverMetrics.remove(metric);
            else
                noReceiverMetrics.remove(metric);
            DispatchPolicyListener listenerInstance = listener.get();
            if (listenerInstance != null)
                listenerInstance.dispatchQueueRemoved(metric.getQueueName());
        } finally {
            lock.writeLock().unlock();
        }

    }

    public ClusteredQueueMetric getLocalMetric() {
        lock.readLock().lock();
        try {
            int rcount = receiverMetrics.size();
            List cloned = (List) ((LinkedList) localMetrics).clone();
            for (int i = 0; i < cloned.size(); i++) {
                QueueMetric qm = (QueueMetric) cloned.get(i);
                if (qm.hasReceiver())
                    rcount++;
            }
            return new ClusteredQueueMetricImpl(clusteredQueueName, cloned, rcount > 0);
        } finally {
            lock.readLock().unlock();
        }

    }

    public void addMetric(String routerName, ClusteredQueueMetric metric) {
        lock.writeLock().lock();
        try {
            removeRouterMetrics(routerName, receiverMetrics);
            removeRouterMetrics(routerName, noReceiverMetrics);
            List list = metric.getQueueMetrics();
            if (list != null) {
                for (int i = 0; i < list.size(); i++) {
                    QueueMetric qm = (QueueMetric) list.get(i);
                    qm.setRouterName(routerName);
                    if (qm.hasReceiver())
                        receiverMetrics.add(qm);
                    else
                        noReceiverMetrics.add(qm);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void removeMetric(String routerName) {
        lock.writeLock().lock();
        try {
            removeRouterMetrics(routerName, receiverMetrics);
            removeRouterMetrics(routerName, noReceiverMetrics);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public boolean isReceiverSomewhere() {
        lock.readLock().lock();
        try {
            return receiverMetrics.size() > 0;
        } finally {
            lock.readLock().unlock();
        }

    }

    public boolean isMessageBasedDispatch() {
        return false;
    }

    public String getNextSendQueue() {
        lock.writeLock().lock();
        try {
            if (receiverMetrics.size() > 0) {
                nextReceiver.getAndIncrement();
                if (nextReceiver.get() > receiverMetrics.size() - 1)
                    nextReceiver.set(0);
                return ((QueueMetric) receiverMetrics.get(nextReceiver.get())).getQueueName();
            }
            if (noReceiverMetrics.size() > 0) {
                nextNoReceiver.getAndIncrement();
                if (nextNoReceiver.get() > noReceiverMetrics.size() - 1)
                    nextNoReceiver.set(0);
                return ((QueueMetric) noReceiverMetrics.get(nextNoReceiver.get())).getQueueName();
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public String getNextSendQueue(MessageImpl message) {
        return getNextSendQueue();
    }

    public String getNextReceiveQueue() {
        lock.writeLock().lock();
        try {
            if (localMetrics.size() == 0)
                return null;
            nextLocal.getAndIncrement();
            if (nextLocal.get() > localMetrics.size() - 1)
                nextLocal.set(0);
            return ((QueueMetric) localMetrics.get(nextLocal.get())).getQueueName();
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void close() {
        lock.writeLock().lock();
        try {
            localMetrics.clear();
            receiverMetrics.clear();
            noReceiverMetrics.clear();
            nextLocal.set(-1);
            nextReceiver.set(-1);
            nextNoReceiver.set(-1);
        } finally {
            lock.writeLock().unlock();
        }

    }
}
