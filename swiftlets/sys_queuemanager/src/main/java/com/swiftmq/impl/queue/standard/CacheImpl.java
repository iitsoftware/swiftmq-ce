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

import com.swiftmq.jms.MessageImpl;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.store.NonPersistentStore;
import com.swiftmq.swiftlet.store.PersistentStore;
import com.swiftmq.swiftlet.store.StoreEntry;

import java.util.Map;

public class CacheImpl implements Cache
{
  volatile int maxMessages = 0;
  volatile long maxBytes = -1;
  Map cacheTable = null;
  PersistentStore pStore = null;
  NonPersistentStore swapStore = null;
  long currentBytes = 0;

  public CacheImpl(int maxMessages, int maxBytesKB, PersistentStore pStore, NonPersistentStore swapStore)
  {
    this.maxMessages = maxMessages;
    this.maxBytes = maxBytesKB == -1 ? Long.MAX_VALUE : maxBytesKB * 1024;
    this.pStore = pStore;
    this.swapStore = swapStore;
  }

  public Map getCacheTable()
  {
    return cacheTable;
  }

  public void setCacheTable(Map cacheTable)
  {
    this.cacheTable = cacheTable;
  }

  public void setMaxMessages(int maxMessages)
  {
    this.maxMessages = maxMessages;
  }

  public int getMaxMessages()
  {
    return maxMessages;
  }

  public int getMaxBytesKB()
  {
    if (maxBytes == Long.MAX_VALUE)
      return -1;
    return (int) (maxBytes / 1024);
  }

  public void setMaxBytesKB(int maxBytesKB)
  {
    this.maxBytes = maxBytesKB == -1 ? Long.MAX_VALUE : maxBytesKB * 1024;
  }

  public int getCurrentMessages()
  {
    return cacheTable == null ? 0 : cacheTable.size();
  }

  public int getCurrentBytesKB()
  {
    return (int) (currentBytes / 1024);
  }

  public void put(StoreId storeId, MessageImpl message)
      throws Exception
  {
    long msgSize = message.getMessageLength();
    storeId.setMsgSize(msgSize);
    if (cacheTable.size() < maxMessages && currentBytes + msgSize < maxBytes)
    {
      cacheTable.put(storeId, new MessageEntry(storeId, message));
      currentBytes += msgSize;
    } else
    {
      // if message is PERSISTENT it is already stored in backstore, so we store
      // only NON_PERSTINENT messages if it is not already in the cache (in case of
      // rollback, it is!)
      if (!storeId.isPersistent() && cacheTable.get(storeId) == null)
      {
        StoreEntry se = new StoreEntry();
        se.priority = storeId.getPriority();
        se.deliveryCount = storeId.getDeliveryCount();
        se.expirationTime = storeId.getExpirationTime();
        se.message = message;
        swapStore.insert(se);
        se.message = null;
        storeId.setPersistentKey(se);
      }
    }
  }

  public MessageEntry get(StoreId storeId)
      throws Exception
  {
    MessageEntry me = (MessageEntry) cacheTable.get(storeId);
    if (me == null)
    {
      me = new MessageEntry();
      me.setMessageIndex(storeId);
      if (storeId.isPersistent())
        me.setMessage(((StoreEntry) pStore.get(((StoreEntry) storeId.getPersistentKey()).key)).message);
      else
        me.setMessage(((StoreEntry) swapStore.get(((StoreEntry) storeId.getPersistentKey()).key)).message);
      storeId.setMsgSize(me.getMessage().getMessageLength());
    }
    return me;
  }

  public void remove(StoreId storeId)
      throws Exception
  {
    MessageEntry entry = (MessageEntry) cacheTable.remove(storeId);
    if (entry == null)
    {
      // remove NON_PERSISTENT_MESSAGE from backstore
      if (!storeId.isPersistent())
        swapStore.delete(((StoreEntry) storeId.getPersistentKey()).key);
    } else
      currentBytes -= entry.getMessage().getMessageLength();
  }

  public void clear()
  {
    cacheTable.clear();
    currentBytes = 0;
  }

  public String toString()
  {
    return "[CacheImpl, maxMessages=" + maxMessages + ", currentMessages=" + (cacheTable == null ? 0 : cacheTable.size()) + ", maxBytesKB = " + getMaxBytesKB() + ", currentBytesKB=" + getCurrentBytesKB() + ", pStore=" + pStore + ", swapStore=" + swapStore + "]";
  }
}

