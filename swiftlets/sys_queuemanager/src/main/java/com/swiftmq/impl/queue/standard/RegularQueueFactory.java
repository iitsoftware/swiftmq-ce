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
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueFactory;
import com.swiftmq.swiftlet.store.NonPersistentStore;
import com.swiftmq.swiftlet.store.PersistentStore;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.util.SwiftUtilities;

public class RegularQueueFactory implements QueueFactory
{
  SwiftletContext ctx = null;
  ThreadPool myTP = null;

  RegularQueueFactory(SwiftletContext ctx)
  {
    this.ctx = ctx;
    myTP = ctx.threadpoolSwiftlet.getPool(QueueManagerImpl.TP_TIMEOUTPROC);
    /*{evaltimer3}*/
  }

  public AbstractQueue createQueue(String queueName, Entity queueEntity)
      throws QueueException
  {
    PersistentStore pStore = null;
    NonPersistentStore nStore = null;

    try
    {
      pStore = ctx.storeSwiftlet.getPersistentStore(queueName);
      nStore = ctx.storeSwiftlet.getNonPersistentStore(queueName);
    } catch (Exception e)
    {
      e.printStackTrace();
      throw new QueueException(e.toString());
    }

    Property prop = queueEntity.getProperty(QueueManagerImpl.PROP_CACHE_SIZE);
    int cacheSize = ((Integer) prop.getValue()).intValue();
    prop = queueEntity.getProperty(QueueManagerImpl.PROP_CACHE_SIZE_BYTES_KB);
    int cacheSizeBytesKB = ((Integer) prop.getValue()).intValue();
    Cache cache = new CacheImpl(cacheSize, cacheSizeBytesKB, pStore, nStore);
    cache.setCacheTable(ctx.cacheTableFactory.createCacheTable(queueName, cacheSize));
    prop = queueEntity.getProperty(QueueManagerImpl.PROP_CACHE_SIZE);
    prop.setPropertyChangeListener(new PropertyChangeAdapter(cache)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        Cache myCache = (Cache) configObject;
        myCache.setMaxMessages(((Integer) newValue).intValue());
      }
    });
    prop = queueEntity.getProperty(QueueManagerImpl.PROP_CACHE_SIZE_BYTES_KB);
    prop.setPropertyChangeListener(new PropertyChangeAdapter(cache)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        Cache myCache = (Cache) configObject;
        myCache.setMaxBytesKB(((Integer) newValue).intValue());
      }
    });

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_CLEANUP_INTERVAL);
    long cleanUp = ((Long) prop.getValue()).longValue();

    MessageQueue mq = ctx.messageQueueFactory.createMessageQueue(ctx, queueName, cache, pStore, nStore, cleanUp, myTP);

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_MESSAGES_MAXIMUM);
    int maxMessages = ((Integer) prop.getValue()).intValue();
    mq.setMaxMessages(maxMessages);
    prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        MessageQueue myMq = (MessageQueue) configObject;
        myMq.setMaxMessages(((Integer) newValue).intValue());
      }
    });

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_PERSISTENCE);
    int pm = SwiftUtilities.persistenceModeToInt((String) prop.getValue());
    mq.setPersistenceMode(pm);
    prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        MessageQueue myMq = (MessageQueue) configObject;
        myMq.setPersistenceMode(SwiftUtilities.persistenceModeToInt((String) newValue));
      }
    });

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_FLOWCONTROL_QUEUE_SIZE);
    int fcQueueSize = ((Integer) prop.getValue()).intValue();
    if (fcQueueSize >= 0)
      mq.setFlowController(new FlowControllerImpl(fcQueueSize, ctx.queueManager.getMaxFlowControlDelay()));
    prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        MessageQueue myMq = (MessageQueue) configObject;
        int newFcQueueSize = ((Integer) newValue).intValue();
        if (newFcQueueSize >= 0)
          myMq.setFlowController(new FlowControllerImpl(newFcQueueSize, ctx.queueManager.getMaxFlowControlDelay()));
        else
          myMq.setFlowController(null);
      }
    });

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_DUPLICATE_DETECTION_ENABLED);
    mq.setDuplicateDetectionEnabled(((Boolean) prop.getValue()).booleanValue());
    prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        MessageQueue myMq = (MessageQueue) configObject;
        myMq.setDuplicateDetectionEnabled(((Boolean) newValue).booleanValue());
      }
    });

    prop = queueEntity.getProperty(QueueManagerImpl.PROP_DUPLICATE_DETECTION_BACKLOG_SIZE);
    mq.setDuplicateDetectionBacklogSize(((Integer) prop.getValue()).intValue());
    prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        MessageQueue myMq = (MessageQueue) configObject;
        myMq.setDuplicateDetectionBacklogSize(((Integer) newValue).intValue());
      }
    });
    prop = queueEntity.getProperty(QueueManagerImpl.PROP_CONSUMER);
    if (prop != null)
    {
      mq.setConsumerMode(ctx.consumerModeInt((String) prop.getValue()));
      prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
      {
        public void propertyChanged(Property property, Object oldValue, Object newValue)
            throws PropertyChangeException
        {
          MessageQueue myMq = (MessageQueue) configObject;
          int cm = ctx.consumerModeInt((String) newValue);
          if (cm == AbstractQueue.EXCLUSIVE && myMq.getReceiverCount() > 1)
            throw new PropertyChangeException("Can't set EXCLUSIVE consumer mode - queue has already '" + myMq.getReceiverCount() + "' receivers");
          myMq.setConsumerMode(cm);
        }
      });
    }
    prop = queueEntity.getProperty(QueueManagerImpl.PROP_MONITOR_ALERT_THRESHOLD);
    if (prop != null)
    {
      mq.setMonitorAlertThreshold(((Integer) prop.getValue()).intValue());
      prop.setPropertyChangeListener(new PropertyChangeAdapter(mq)
      {
        public void propertyChanged(Property property, Object oldValue, Object newValue)
            throws PropertyChangeException
        {
          MessageQueue myMq = (MessageQueue) configObject;
          myMq.setMonitorAlertThreshold(((Integer) newValue).intValue());
        }
      });
    }

    return mq;
  }
}

