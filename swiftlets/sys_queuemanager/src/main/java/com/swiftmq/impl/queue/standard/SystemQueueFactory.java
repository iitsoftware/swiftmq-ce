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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueFactory;
import com.swiftmq.swiftlet.store.NonPersistentStore;
import com.swiftmq.swiftlet.store.PersistentStore;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.util.SwiftUtilities;

public class SystemQueueFactory implements QueueFactory {
    SwiftletContext ctx = null;
    ThreadPool myTP = null;

    SystemQueueFactory(SwiftletContext ctx) {
        this.ctx = ctx;
        myTP = ctx.threadpoolSwiftlet.getPool(QueueManagerImpl.TP_TIMEOUTPROC);
        /*{evaltimer3}*/
    }

    public AbstractQueue createQueue(String queueName, Entity queueController)
            throws QueueException {
        PersistentStore pStore = null;
        NonPersistentStore nStore = null;

        try {
            pStore = ctx.storeSwiftlet.getPersistentStore(queueName);
            nStore = ctx.storeSwiftlet.getNonPersistentStore(queueName);
        } catch (Exception e) {
            throw new QueueException(e.toString());
        }

        Property prop = queueController.getProperty(QueueManagerImpl.PROP_CACHE_SIZE);
        int cacheSize = ((Integer) prop.getValue()).intValue();
        prop = queueController.getProperty(QueueManagerImpl.PROP_CACHE_SIZE_BYTES_KB);
        int cacheSizeBytesKB = ((Integer) prop.getValue()).intValue();

        Cache cache = new CacheImpl(cacheSize, cacheSizeBytesKB, pStore, nStore);
        cache.setCacheTable(ctx.cacheTableFactory.createCacheTable(queueName, cacheSize));

        prop = queueController.getProperty(QueueManagerImpl.PROP_CLEANUP_INTERVAL);
        long cleanUp = ((Long) prop.getValue()).longValue();

        MessageQueue mq = ctx.messageQueueFactory.createMessageQueue(ctx, queueName, cache, pStore, nStore, cleanUp, myTP);

        prop = queueController.getProperty(QueueManagerImpl.PROP_MESSAGES_MAXIMUM);
        int maxMessages = ((Integer) prop.getValue()).intValue();
        mq.setMaxMessages(maxMessages);

        prop = queueController.getProperty(QueueManagerImpl.PROP_PERSISTENCE);
        int pm = SwiftUtilities.persistenceModeToInt((String) prop.getValue());
        mq.setPersistenceMode(pm);

        prop = queueController.getProperty(QueueManagerImpl.PROP_FLOWCONTROL_QUEUE_SIZE);
        int fcQueueSize = ((Integer) prop.getValue()).intValue();
        if (fcQueueSize >= 0)
            mq.setFlowController(new FlowControllerImpl(fcQueueSize, ctx.queueManager.getMaxFlowControlDelay()));

        prop = queueController.getProperty(QueueManagerImpl.PROP_DUPLICATE_DETECTION_ENABLED);
        mq.setDuplicateDetectionEnabled(((Boolean) prop.getValue()).booleanValue());

        prop = queueController.getProperty(QueueManagerImpl.PROP_DUPLICATE_DETECTION_BACKLOG_SIZE);
        mq.setDuplicateDetectionBacklogSize(((Integer) prop.getValue()).intValue());

        prop = queueController.getProperty(QueueManagerImpl.PROP_CONSUMER);
        mq.setConsumerMode(ctx.consumerModeInt((String) prop.getValue()));

        mq.setQueueController(queueController);
        return mq;
    }
}

