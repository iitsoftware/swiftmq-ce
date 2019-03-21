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
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MessageGroupDispatchPolicy implements DispatchPolicy, DispatchPolicyListener, TimerListener
{
  SwiftletContext ctx = null;
  Entity myEntity = null;
  String clusteredQueueName = null;
  DispatchPolicy parent = null;
  boolean enabled = false;
  String propName = null;
  long expiration = 0;
  long cleanupInterval = 0;
  Map groups = new HashMap();

  public MessageGroupDispatchPolicy(SwiftletContext ctx, Entity myEntity, String clusteredQueueName, DispatchPolicy parent)
  {
    this.ctx = ctx;
    this.myEntity = myEntity;
    this.clusteredQueueName = clusteredQueueName;
    this.parent = parent;
    init();
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/created");
    restart();
  }

  private void init()
  {
    Property prop = myEntity.getProperty("message-group-enabled");
    enabled = ((Boolean) prop.getValue()).booleanValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {

      public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException
      {
        enabled = ((Boolean) newValue).booleanValue();
        if (enabled)
          restart();
        else
          reset();
      }
    });
    prop = myEntity.getProperty("message-group-property");
    propName = (String) prop.getValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {

      public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException
      {
        if (enabled)
          throw new PropertyChangeException("Please disable message grouping for this clustered queue before you change this property!");
        propName = (String) newValue;
      }
    });
    prop = myEntity.getProperty("message-group-expiration");
    expiration = ((Long) prop.getValue()).longValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {

      public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException
      {
        expiration = ((Long) newValue).longValue();
        if (enabled && expiration != 0)
          checkExpiration();
      }
    });
    prop = myEntity.getProperty("message-group-expiration-cleanup-interval");
    cleanupInterval = ((Long) prop.getValue()).longValue();
    prop.setPropertyChangeListener(new PropertyChangeListener()
    {

      public void propertyChanged(Property property, Object oldValue, Object newValue) throws PropertyChangeException
      {
        cleanupInterval = changeCleanupInterval(((Long) newValue).longValue());
      }
    });
  }

  protected synchronized void reset()
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/reset");
    if (cleanupInterval > 0)
      ctx.timerSwiftlet.removeTimerListener(this);
    groups.clear();
  }

  private synchronized void restart()
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/restart");
    if (cleanupInterval > 0)
      ctx.timerSwiftlet.addTimerListener(cleanupInterval, this);
  }

  private synchronized void checkExpiration()
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/checkExpiration ...");
    if (enabled)
    {
      long current = System.currentTimeMillis();
      for (Iterator iter = groups.entrySet().iterator(); iter.hasNext(); )
      {
        MessageGroupEntry entry = (MessageGroupEntry) ((Map.Entry) iter.next()).getValue();
        if (entry.getLastDispatchTime() + expiration <= current)
        {
          if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(clusteredQueueName, toString() + "/checkExpiration, remove=" + entry);
          iter.remove();
          deleteGroupEntry(entry);
        }
      }
    }
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/checkExpiration ...");
  }

  private synchronized long changeCleanupInterval(long newInterval)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/changeCleanupInterval, newInterval=" + newInterval);
    if (enabled)
    {
      if (cleanupInterval > 0)
        ctx.timerSwiftlet.removeTimerListener(this);
      if (newInterval > 0)
        ctx.timerSwiftlet.addTimerListener(newInterval, this);
    }
    return newInterval;
  }

  private synchronized void removeQueue(String queueName)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/removeQueue, queueName=" + queueName + " ...");
    if (enabled)
    {
      for (Iterator iter = groups.entrySet().iterator(); iter.hasNext(); )
      {
        MessageGroupEntry entry = (MessageGroupEntry) ((Map.Entry) iter.next()).getValue();
        if (entry.getQueueName().equals(queueName))
        {
          if (ctx.queueSpace.enabled)
            ctx.queueSpace.trace(clusteredQueueName, toString() + "/removeQueue, remove=" + entry);
          iter.remove();
          deleteGroupEntry(entry);
        }
      }
    }
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/removeQueue, queueName=" + queueName + " done");
  }

  public String getClusteredQueueName()
  {
    return clusteredQueueName;
  }

  public void setGroups(Map groups)
  {
    this.groups = groups;
  }

  public Map getGroups()
  {
    return groups;
  }

  public void performTimeAction()
  {
    checkExpiration();
  }

  public void setDispatchPolicyListener(DispatchPolicyListener l)
  {
    // nothing
  }

  public void dispatchQueueRemoved(String queueName)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/dispatchQueueRemoved, queueName=" + queueName + " ...");
    removeQueue(queueName);
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/dispatchQueueRemoved, queueName=" + queueName + " done");
  }

  public void addLocalMetric(QueueMetric metric)
  {
    parent.addLocalMetric(metric);
  }

  public void removeLocalMetric(QueueMetric metric)
  {
    parent.removeLocalMetric(metric);
  }

  public ClusteredQueueMetric getLocalMetric()
  {
    return parent.getLocalMetric();
  }

  public void addMetric(String routerName, ClusteredQueueMetric metric)
  {
    parent.addMetric(routerName, metric);
  }

  public void removeMetric(String routerName)
  {
    parent.removeMetric(routerName);
  }

  public boolean isReceiverSomewhere()
  {
    return parent.isReceiverSomewhere();
  }

  public boolean isMessageBasedDispatch()
  {
    return enabled;
  }

  public String getNextSendQueue()
  {
    return parent.getNextSendQueue();
  }

  protected MessageGroupEntry createNewGroupEntry(Object value)
  {
    return new MessageGroupEntry(value, getNextSendQueue());
  }

  protected void updateGroupEntry(MessageGroupEntry entry)
  {
    entry.setLastDispatchTime(System.currentTimeMillis());
  }

  protected void deleteGroupEntry(MessageGroupEntry entry)
  {
  }

  public synchronized String getNextSendQueue(MessageImpl message)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/getNextSendQueue(msg), propName=" + propName + " ...");
    Object value = null;
    try
    {
      value = message.getObjectProperty(propName);
    } catch (JMSException e)
    {
    }
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/getNextSendQueue(msg), propName=" + propName + ", value=" + value);
    if (value == null)
      return parent.getNextSendQueue();
    // Determine group queue
    MessageGroupEntry entry = (MessageGroupEntry) groups.get(value);
    if (entry == null)
    {
      entry = createNewGroupEntry(value);
      groups.put(value, entry);
    } else
      updateGroupEntry(entry);
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/getNextSendQueue(msg), propName=" + propName + ", value=" + value + ", returns queue=" + entry.getQueueName());
    return entry.getQueueName();
  }

  public String getNextReceiveQueue()
  {
    return parent.getNextReceiveQueue();
  }

  public void receiverCountChanged(AbstractQueue abstractQueue, int receiverCount)
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/receiverCountChanged, queue=" + abstractQueue.getQueueName() + ", count=" + receiverCount + " ...");
    if (receiverCount == 0)
      removeQueue(abstractQueue.getQueueName());
    parent.receiverCountChanged(abstractQueue, receiverCount);
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/receiverCountChanged, queue=" + abstractQueue.getQueueName() + ", count=" + receiverCount + " done");
  }

  public void close()
  {
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/close ...");
    reset();
    parent.close();
    if (ctx.queueSpace.enabled)
      ctx.queueSpace.trace(clusteredQueueName, toString() + "/close done");
  }

  public String toString()
  {
    return "MessageGroupDispatchPolicy, enabled=" + enabled;
  }

}
