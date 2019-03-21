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

package com.swiftmq.impl.topic.standard;

import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.queue.Selector;
import com.swiftmq.swiftlet.store.DurableStoreEntry;

import java.io.Serializable;

public class DurableSubscription implements Comparable, Serializable
{
  String clientId;
  String durableName;
  String queueName;
  String topicName;
  boolean noLocal;
  String condition;
  transient Selector selector;
  transient TopicSubscription topicSubscription = null;

  public DurableSubscription(String clientId, String durableName, String topicName,
                             Selector selector, boolean noLocal)
  {
    this.clientId = stripRouterName(clientId);
    this.durableName = durableName;
    this.topicName = topicName;
    this.selector = selector;
    this.condition = selector != null ? selector.getConditionString() : null;
    this.noLocal = noLocal;
    queueName = createDurableQueueName(clientId, durableName);
  }

  public DurableSubscription(DurableStoreEntry storeEntry)
  {
    MessageSelector ms = null;
    if (storeEntry.getSelector() != null)
    {
      try
      {
        ms = new MessageSelector(storeEntry.getSelector());
        ms.compile();
      } catch (Exception ignored)
      {
      }
    }
    this.clientId = stripRouterName(storeEntry.getClientId());
    this.durableName = storeEntry.getDurableName();
    this.topicName = storeEntry.getTopicName();
    this.selector = ms;
    this.condition = selector != null ? selector.getConditionString() : null;
    this.noLocal = storeEntry.isNoLocal();
    queueName = createDurableQueueName(clientId, durableName);
  }

  private static String stripRouterName(String s)
  {
    if (s.indexOf('@') != -1)
      return s.substring(0, s.indexOf('@'));
    return s;
  }

  public DurableStoreEntry getDurableStoreEntry()
  {
    return new DurableStoreEntry(clientId, durableName, topicName, condition, noLocal);
  }

  public static String createDurableQueueName(String clientId, String durableName)
  {
    return stripRouterName(clientId) + "$" + durableName;
  }

  public String getClientId()
  {
    return clientId;
  }

  public String getDurableName()
  {
    return durableName;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public String getTopicName()
  {
    return topicName;
  }

  public Selector getSelector()
  {
    if (condition != null && selector == null)
    {
      try
      {
        MessageSelector ms = new MessageSelector(condition);
        ms.compile();
        selector = ms;
      } catch (Exception ignored)
      {
      }
    }
    return selector;
  }

  public boolean isNoLocal()
  {
    return noLocal;
  }

  public void setTopicSubscription(TopicSubscription topicSubscription)
  {
    this.topicSubscription = topicSubscription;
    if (topicSubscription != null)
      topicSubscription.setDurable(true);
  }

  public TopicSubscription getTopicSubscription()
  {
    return topicSubscription;
  }

  public boolean hasChanged(String newTopicName,
                            Selector newSelector,
                            boolean newNoLocal)
  {
    return !topicName.equals(newTopicName) ||
        selector != null && newSelector == null ||
        selector == null && newSelector != null ||
        selector != null && newSelector != null &&
            !selector.getConditionString().equals(newSelector.getConditionString()) ||
        noLocal != newNoLocal;
  }

  public boolean equals(Object o)
  {
    return compareTo(o) == 0;
  }

  public int compareTo(Object o)
  {
    DurableSubscription thatDurable = (DurableSubscription) o;
    return queueName.compareTo(thatDurable.getQueueName());
  }

  public String toString()
  {
    StringBuffer s = new StringBuffer();
    s.append("[DurableSubscription, clientId=");
    s.append(clientId);
    s.append(", durableName=");
    s.append(durableName);
    s.append(", topicName=");
    s.append(topicName);
    s.append(", selector=");
    s.append(getSelector());
    s.append(", noLocal=");
    s.append(noLocal);
    s.append(", queueName=");
    s.append(queueName);
    s.append("]");
    return s.toString();
  }
}

