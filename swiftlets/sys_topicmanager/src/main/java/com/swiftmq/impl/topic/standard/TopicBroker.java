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

import com.swiftmq.impl.topic.standard.announce.TopicInfo;
import com.swiftmq.impl.topic.standard.announce.TopicInfoFactory;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.TopicImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.queue.AbstractQueue;
import com.swiftmq.swiftlet.queue.QueueException;
import com.swiftmq.swiftlet.queue.QueueTransactionClosedException;
import com.swiftmq.swiftlet.queue.Selector;
import com.swiftmq.swiftlet.store.CompositeStoreTransaction;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.collection.ArrayListTool;
import com.swiftmq.tools.concurrent.AsyncCompletionCallback;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicBroker extends AbstractQueue
{
  TopicManagerContext ctx = null;
  String tracePrefix = null;
  List topicEntries = new ArrayList();

  HashMap subscriptions = new HashMap();
  String rootTopic;
  String[] rootTokenized = null;
  ArrayList transactions = new ArrayList();
  BitSet brokerSubscriberIds = null;
  DataByteArrayOutputStream dbos = null;
  DataByteArrayInputStream dbis = null;
  SlowSubscriberCondition slowSubscriberCondition = null;
  Lock lock = new ReentrantLock();
  Condition asyncFinished = null;
  AtomicBoolean asyncActive = new AtomicBoolean(false);

  protected TopicBroker(TopicManagerContext ctx, String rootTopic)
  {
    this.ctx = ctx;
    this.rootTopic = rootTopic;
    rootTokenized = new String[]{rootTopic};
    dbos = new DataByteArrayOutputStream(4096);
    dbis = new DataByteArrayInputStream();
    asyncFinished = lock.newCondition();
    tracePrefix = "TopicBroker '" + rootTopic + "': ";
    brokerSubscriberIds = new TraceableBitSet(4096);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "created");
  }

  private void lockAndWaitAsyncFinished()
  {
    lock.lock();
    while (asyncActive.get())
      asyncFinished.awaitUninterruptibly();
  }

  protected String getRootTopic()
  {
    return rootTopic;
  }

  public void setSlowSubscriberCondition(SlowSubscriberCondition slowSubscriberCondition)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "setSlowSubscriberCondition: " + slowSubscriberCondition);
      this.slowSubscriberCondition = slowSubscriberCondition;
    } finally
    {
      lock.unlock();
    }
  }

  protected void addTopic(String topic, String[] tokenizedName)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addTopic: '" + topic + "'");
      topicEntries.add(new TopicEntry(topic, tokenizedName));
      // Broadcast creation info
      if (ctx.announceSender != null)
        ctx.announceSender.topicCreated(topic);
    } finally
    {
      lock.unlock();
    }
  }

  protected void removeTopic(String topic, String[] tokenizedName)
  {
    lockAndWaitAsyncFinished();
    try
    {
      for (int i = 0; i < topicEntries.size(); i++)
      {
        TopicEntry entry = (TopicEntry) topicEntries.get(i);
        if (entry.getTopicName().equals(topic))
        {
          topicEntries.remove(entry);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "removeTopic: '" + topic + "'");
          break;
        }
      }
      // Broadcast removal info
      if (ctx.announceSender != null)
        ctx.announceSender.topicRemoved(topic);
    } finally
    {
      lock.unlock();
    }
  }

  protected String[] getTopicNames()
  {
    lockAndWaitAsyncFinished();
    try
    {
      String[] topicNames = new String[topicEntries.size()];
      for (int i = 0; i < topicEntries.size(); i++)
      {
        TopicEntry entry = (TopicEntry) topicEntries.get(i);
        topicNames[i] = entry.getTopicName();
      }
      return topicNames;
    } finally
    {
      lock.unlock();
    }
  }

  protected String getTopicName(String[] tokenizedPredicate)
  {
    lockAndWaitAsyncFinished();
    try
    {
      String topicName = null;
      for (int i = 0; i < topicEntries.size(); i++)
      {
        TopicEntry entry = (TopicEntry) topicEntries.get(i);
        if (entry.isMatch(tokenizedPredicate))
        {
          topicName = entry.getTopicName();
          break;
        }
      }
      return topicName;
    } finally
    {
      lock.unlock();
    }
  }

  protected List getMatchedTopics(String[] tokenizedPredicate)
  {
    lockAndWaitAsyncFinished();
    try
    {
      ArrayList r = null;
      for (int i = 0; i < topicEntries.size(); i++)
      {
        TopicEntry entry = (TopicEntry) topicEntries.get(i);
        if (entry.isMatch(tokenizedPredicate))
        {
          if (r == null)
            r = new ArrayList();
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "getMatchedTopics: '" + TopicManagerImpl.concatName(tokenizedPredicate) + "' returns '" + entry.getTopicName() + "'");
          r.add(entry.getTokenizedName());
        }
      }
      return r;
    } finally
    {
      lock.unlock();
    }
  }

  protected void subscribe(TopicSubscription topicSubscription)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "subscribe " + topicSubscription);
      String topicName = topicSubscription.getTopicName();
      PredicateNode node = (PredicateNode) subscriptions.get(topicName);
      if (node == null)
      {
        node = new PredicateNode(topicName, topicSubscription.getTokenizedName());
        subscriptions.put(topicName, node);
      }
      int bsid = brokerSubscriberIds.nextClearBit(0);
      brokerSubscriberIds.set(bsid);
      topicSubscription.setBrokerSubscriberId(bsid);
      node.addSubscription(topicSubscription);
    } finally
    {
      lock.unlock();
    }
  }

  protected void unsubscribe(TopicSubscription topicSubscription)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "unsubscribe " + topicSubscription);
      brokerSubscriberIds.clear(topicSubscription.getBrokerSubscriberId());
      String topicName = topicSubscription.getTopicName();
      PredicateNode node = (PredicateNode) subscriptions.get(topicName);
      if (node != null)
      {
        node.removeSubscription(topicSubscription, false);
        if (node.isEmpty())
        {
          subscriptions.remove(topicName);
          if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "removing node " + node);
        }
      }
    } finally
    {
      lock.unlock();
    }
  }

  protected void addStaticSubscription(String routerName, boolean keepOnUnsubscribe)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addStaticSubscription " + routerName + ", keep=" + keepOnUnsubscribe);
      TopicInfo topicInfo = TopicInfoFactory.createTopicInfo(routerName, rootTopic, new String[]{rootTopic}, 0);
      TopicSubscription topicSubscription = new TopicSubscription(topicInfo, this);
      PredicateNode node = (PredicateNode) subscriptions.get(topicInfo.getTopicName());
      if (node == null)
      {
        node = new PredicateNode(topicInfo.getTopicName(), topicInfo.getTokenizedPredicate());
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addStaticSubscription " + routerName + ", node not found, create new node: " + node);
        int bsid = brokerSubscriberIds.nextClearBit(0);
        brokerSubscriberIds.set(bsid);
        topicSubscription.setBrokerSubscriberId(bsid);
        topicSubscription.setStaticSubscription(true);
        topicSubscription.setKeepOnUnsubscribe(keepOnUnsubscribe);
        subscriptions.put(topicInfo.getTopicName(), node);
        node.addSubscription(topicSubscription);
      } else
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addStaticSubscription " + routerName + ", node found: " + node);
        TopicSubscription ts = node.getSubscription(topicSubscription);
        if (ts == null)
        {
          int bsid = brokerSubscriberIds.nextClearBit(0);
          brokerSubscriberIds.set(bsid);
          topicSubscription.setBrokerSubscriberId(bsid);
          topicSubscription.setStaticSubscription(true);
          topicSubscription.setKeepOnUnsubscribe(keepOnUnsubscribe);
          subscriptions.put(topicInfo.getTopicName(), node);
          node.addSubscription(topicSubscription);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addStaticSubscription " + routerName + ", subscription not found, create new one: " + topicSubscription);
        } else
        {
          ts.setStaticSubscription(true);
          topicSubscription.setKeepOnUnsubscribe(keepOnUnsubscribe);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addStaticSubscription " + routerName + ", subscription found, mark static: " + ts);
        }
      }
    } finally
    {
      lock.unlock();
    }
  }

  protected void removeStaticSubscription(String routerName)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "removeStaticSubscription " + routerName);
      TopicInfo topicInfo = TopicInfoFactory.createTopicInfo(routerName, rootTopic, new String[]{rootTopic}, 0);
      PredicateNode node = (PredicateNode) subscriptions.get(topicInfo.getTopicName());
      if (node != null)
      {
        TopicSubscription topicSubscription = new TopicSubscription(topicInfo, this);
        TopicSubscription ts = node.getSubscription(topicSubscription);
        if (ts != null)
        {
          brokerSubscriberIds.clear(ts.getBrokerSubscriberId());
          if (ts.isAnnounced())
            ts.setStaticSubscription(false);
          else
            node.removeSubscription(ts, true);
        }
        if (node.isEmpty())
        {
          subscriptions.remove(topicInfo.getTopicName());
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "removeStaticSubscription, removing node " + node);
        }
      }
    } finally
    {
      lock.unlock();
    }
  }

  private void addRemoteSubscriptionUsage(String routerName, String topicName)
  {
    if (ctx.remoteSubscriberList == null)
      return;
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "addRemoteSubscriptionUsage, routerName=" + routerName + ", topicName=" + topicName);
    try
    {
      Entity rEntity = ctx.remoteSubscriberList.getEntity(routerName);
      if (rEntity == null)
      {
        rEntity = ctx.remoteSubscriberList.createEntity();
        rEntity.setName(routerName);
        rEntity.createCommands();
        ctx.remoteSubscriberList.addEntity(rEntity);
      }
      EntityList tEntityList = (EntityList) rEntity.getEntity("topics");
      if (tEntityList.getEntity(topicName) == null)
      {
        Entity entity = tEntityList.createEntity();
        entity.setName(topicName);
        entity.createCommands();
        tEntityList.addEntity(entity);
      }
    } catch (Exception e)
    {
    }
  }

  private void removeRemoteSubscriptionUsage(String routerName, String topicName)
  {
    if (ctx.remoteSubscriberList == null)
      return;
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "removeRemoteSubscriptionUsage, routerName=" + routerName + ", topicName=" + topicName);
    try
    {
      Entity rEntity = ctx.remoteSubscriberList.getEntity(routerName);
      if (rEntity != null)
      {
        EntityList tEntityList = (EntityList) rEntity.getEntity("topics");
        Entity entity = tEntityList.getEntity(topicName);
        if (entity != null)
          tEntityList.removeEntity(entity);
        Map map = tEntityList.getEntities();
        if (map == null || map.size() == 0)
          ctx.remoteSubscriberList.removeEntity(rEntity);
      }
    } catch (Exception e)
    {
    }
  }

  protected void processTopicInfo(TopicInfo topicInfo)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "processTopicInfo: " + topicInfo);
      if (getTopicName(topicInfo.getTokenizedPredicate()) != null)
      {
        PredicateNode node = (PredicateNode) subscriptions.get(topicInfo.getTopicName());
        if (node == null)
        {
          if (topicInfo.getNumberSubscriptions() > 0)
          {
            node = new PredicateNode(topicInfo.getTopicName(), topicInfo.getTokenizedPredicate());
            subscriptions.put(topicInfo.getTopicName(), node);
            TopicSubscription topicSubscription = new TopicSubscription(topicInfo, this);
            int bsid = brokerSubscriberIds.nextClearBit(0);
            brokerSubscriberIds.set(bsid);
            topicSubscription.setBrokerSubscriberId(bsid);
            topicSubscription.setAnnounced(true);
            node.addSubscription(topicSubscription);
          }
        } else
        {
          TopicSubscription topicSubscription = new TopicSubscription(topicInfo, this);
          TopicSubscription ts = node.getSubscription(topicSubscription);
          if (topicInfo.getNumberSubscriptions() > 0)
          {
            if (ts != null)
              ts.setAnnounced(true);
            else
            {
              int bsid = brokerSubscriberIds.nextClearBit(0);
              brokerSubscriberIds.set(bsid);
              topicSubscription.setBrokerSubscriberId(bsid);
              topicSubscription.setAnnounced(true);
              node.addSubscription(topicSubscription);
            }
          } else
          {
            if (ts != null)
            {
              if (ts.isStaticSubscription())
                ts.setAnnounced(false);
              else
                node.removeSubscription(ts, false);
            }
          }
        }
        if (topicInfo.getNumberSubscriptions() > 0)
          addRemoteSubscriptionUsage(topicInfo.getRouterName(), topicInfo.getTopicName());
        else
          removeRemoteSubscriptionUsage(topicInfo.getRouterName(), topicInfo.getTopicName());
      } else if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "processTopicInfo, no topic for: " + topicInfo);
    } finally
    {
      lock.unlock();
    }
  }

  /**
   * Start the queue. Will be called from the queue manager. After startup all
   * persistent messages stored in the queue must be available.
   *
   * @throws QueueException on error
   */
  public void startQueue()
      throws QueueException
  {
  }

  /**
   * Stops the queue. Will be called from the queue manager
   *
   * @throws QueueException on error
   */
  public void stopQueue()
      throws QueueException
  {
  }

  public void lockQueue(Object txId)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "lockQueue, txId=" + txId);
    ((TopicTransaction) txId).lockQueues();
  }

  public void unlockQueue(Object txId, boolean markAsyncActive)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "unlockQueue, txId=" + txId);
    ((TopicTransaction) txId).unlockQueues();
  }

  public void setCompositeStoreTransaction(Object txId, CompositeStoreTransaction ct)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "setCompositeStoreTransaction, txId=" + txId + ", ct=" + ct);
    ((TopicTransaction) txId).setParentTx(ct);
  }

  public boolean hasReceiver(MessageImpl message)
  {
    return false;
  }

  /**
   * Returns if the queue is running or not
   *
   * @return true/false
   */
  public boolean isRunning()
  {
    return (true); // NYI
  }

  /**
   * Creates a new push transaction and returns a unique transaction id
   *
   * @return transaction id
   * @throws QueueException on error
   */
  public Object createPushTransaction()
      throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      TopicTransaction transaction = null;
      int transactionId = ArrayListTool.setFirstFreeOrExpand(transactions, null);
      transaction = new TopicTransaction(ctx, transactionId);
      transactions.set(transactionId, transaction);
      return transaction;
    } finally
    {
      lock.unlock();
    }
  }

  /**
   * Creates a new pull transaction and returns a unique transaction id
   *
   * @return transaction id
   * @throws QueueException on error
   */
  public Object createPullTransaction()
      throws QueueException
  {
    throw new QueueException("operation is not supported; this is a TopicBroker!");
  }

  public void prepare(Object localTransactionId, XidImpl globalTransactionId) throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "prepare, globalTxId=" + globalTransactionId);
      TopicTransaction transaction = (TopicTransaction) localTransactionId;
      transaction.prepare(globalTransactionId);
      transactions.set(transaction.getTransactionId(), null);
    } catch (Exception e)
    {
      throw new QueueException(e.getMessage());
    } finally
    {
      lock.unlock();
    }
  }

  public void commit(Object localTransactionId, XidImpl globalTransactionId) throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "commit, globalTxId=" + globalTransactionId);
      TopicTransaction transaction = (TopicTransaction) localTransactionId;
      long delay = transaction.commit(globalTransactionId);
      TopicFlowController fc = (TopicFlowController) getFlowController();
      if (fc != null)
        fc.setLastDelay(delay);
      transactions.set(transaction.getTransactionId(), null);
    } catch (Exception e)
    {
      throw new QueueException(e.getMessage());
    } finally
    {
      lock.unlock();
    }

  }

  /**
   * Commit the transaction with the given transaction id
   *
   * @param transactionId transaction id
   * @throws QueueException on error
   */
  public void commit(Object transactionId)
      throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "commit");
      TopicTransaction transaction = (TopicTransaction) transactionId;
      long delay = transaction.commit();
      TopicFlowController fc = (TopicFlowController) getFlowController();
      if (fc != null)
        fc.setLastDelay(delay);
      transactions.set(transaction.getTransactionId(), null);
    } catch (Exception e)
    {
      throw new QueueException(e.getMessage());
    } finally
    {
      lock.unlock();
    }
  }

  public void commit(Object transactionId, AsyncCompletionCallback callback)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "commit (callback)");
      final TopicTransaction transaction = (TopicTransaction) transactionId;
      asyncActive.set(true);
      transaction.commit(new AsyncCompletionCallback(callback)
      {
        public void done(boolean success)
        {
          lock.lock();
          try
          {
            if (success)
            {
              Long delay = (Long) getResult();
              if (delay != null)
              {
                TopicFlowController fc = (TopicFlowController) getFlowController();
                if (fc != null)
                {
                  next.setResult(delay);
                  fc.setLastDelay(delay.longValue());
                }
              }
              transactions.set(transaction.getTransactionId(), null);
            } else
              next.setException(getException());
          } finally
          {
            asyncActive.set(false);;
            asyncFinished.signalAll();
            lock.unlock();
          }
        }
      });
    } finally
    {
      lock.unlock();
    }
  }

  public void rollback(Object localTransactionId, XidImpl globalTransactionId, boolean setRedelivered) throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "rollback, globalTxId=" + globalTransactionId);
      TopicTransaction transaction = (TopicTransaction) localTransactionId;
      transaction.rollback(globalTransactionId);
      transactions.set(transaction.getTransactionId(), null);
    } catch (Exception e)
    {
      throw new QueueException(e.getMessage());
    } finally
    {
      lock.unlock();
    }

  }

  /**
   * Rolls back the transaction with the given transaction id. If
   * the flag <code>setRedelivered</code> is set then the JMS properties for
   * redelivery and delivery count of messages pulled within this transaction
   * are updated
   *
   * @param transactionId  transaction id
   * @param setRedelivered specifies JMS redelivery setting
   * @throws QueueException on error
   */
  public void rollback(Object transactionId, boolean setRedelivered)
      throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "rollback");
      TopicTransaction transaction = (TopicTransaction) transactionId;
      transaction.rollback();
      transactions.set(transaction.getTransactionId(), null);
    } catch (Exception e)
    {
      throw new QueueException(e.getMessage());
    } finally
    {
      lock.unlock();
    }
  }

  public void rollback(Object transactionId, boolean setRedelivered, AsyncCompletionCallback callback)
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "rollback");
      final TopicTransaction transaction = (TopicTransaction) transactionId;
      asyncActive.set(true);
      transaction.rollback(new AsyncCompletionCallback(callback)
      {
        public void done(boolean success)
        {
          lock.lock();
          try
          {
            transactions.set(transaction.getTransactionId(), null);
            if (!success)
              next.setException(getException());
          } finally
          {
            asyncActive.set(false);
            asyncFinished.signalAll();
            lock.unlock();
          }
        }
      });
      transactions.set(transaction.getTransactionId(), null);
    } finally
    {
      lock.unlock();
    }
  }

  private MessageImpl copyMessage(MessageImpl msg) throws Exception
  {
    dbos.rewind();
    msg.writeContent(dbos);
    dbis.reset();
    dbis.setBuffer(dbos.getBuffer(), 0, dbos.getCount());
    MessageImpl msgCopy = MessageImpl.createInstance(dbis.readInt());
    msgCopy.readContent(dbis);
    return msgCopy;
  }

  private void checkDisconnect(TopicSubscription sub)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + " ...");
    if (sub.isRemote() || sub.isRemovePending())
      return;
    if (ctx.queueManager.isTemporaryQueue(sub.getSubscriberQueueName()))
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + ", is nondurable!");
      if (slowSubscriberCondition.isDisconnectNonDurable())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + ", force disconnect!");
        sub.setRemovePending(true);
        ctx.timerSwiftlet.addInstantTimerListener(500, new NonDurableRemover(sub.getSubscriberQueueName()));
        ctx.logSwiftlet.logWarning("sys$topicmanager", tracePrefix + "Disconnecting slow non-durable subscriber: " + sub);
      }
    } else
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + ", is durable!");
      if (slowSubscriberCondition.isDisconnectDeleteDurable())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + ", force disconnect!");
        sub.setRemovePending(true);
        ctx.logSwiftlet.logWarning("sys$topicmanager", tracePrefix + "Disconnecting slow durable subscriber: " + sub);
        ctx.timerSwiftlet.addInstantTimerListener(500, new DurableRemover(sub.getSubscriberQueueName()));
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "checkDisconnect, sub=" + sub + " done");
  }

  private void publishToNode(TopicTransaction transaction, MessageImpl message, PredicateNode node, boolean publishedLocal)
      throws QueueException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode " + node);
    boolean isPersistent = false;
    try
    {
      isPersistent = message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT;
    } catch (JMSException e)
    {
    }
    List subs = node.getSubscriptions();
    for (int i = 0; i < subs.size(); i++)
    {
      // check all subs (selector, noLocal)
      TopicSubscription sub = (TopicSubscription) subs.get(i);
      if (sub != null)
      {
        boolean exclude = false;
        if (slowSubscriberCondition != null)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", checking slow subscriber condition: " + slowSubscriberCondition);
          try
          {
            exclude = slowSubscriberCondition.isMatch(message.getJMSDeliveryMode(), sub);
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", checking slow subscriber condition: " + slowSubscriberCondition + " returns " + exclude);
          } catch (Exception e)
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", exception checking slow subscriber condition: " + slowSubscriberCondition + ", exception=" + e);
            exclude = false;
          }
        }
        if (exclude)
        {
          checkDisconnect(sub);
        } else
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", checking remote");
          if (!sub.isRemote() || sub.isRemote() && publishedLocal)
          {
            Selector selector = sub.getSelector();
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", checking selector: " + selector);
            if (selector == null || selector.isSelected(message))
            {
              String clientId = null;
              try
              {
                clientId = message.getStringProperty(MessageImpl.PROP_CLIENT_ID);
              } catch (Exception ignored)
              {
              }
              String subClientId = sub.getActiveLogin() != null ? sub.getActiveLogin().getClientId() : null;
              if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", checking clientId: " + clientId + " subClientId: " + subClientId);
              if ((!sub.isNoLocal()) ||
                  (sub.isNoLocal() &&
                      (clientId == null ||
                          sub.getActiveLogin() != null && !clientId.equals(subClientId))))
              {
                if (ctx.traceSpace.enabled)
                  ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", that seems ok...");
                // check if sub has already transaction
                TopicSubscriberTransaction subTransaction = transaction.getTopicSubscriberTransaction(sub.getBrokerSubscriberId());
                if (subTransaction == null)
                {
                  if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", creating new subscriber transaction");
                  // if not, create one and store it in TopicTransaction
                  try
                  {
                    subTransaction = sub.createTransaction();
                  } catch (Exception e)
                  {
                    throw new QueueException(e.getMessage());
                  }
                  transaction.setTopicSubscriberTransaction(sub.getBrokerSubscriberId(), subTransaction);
                }

                // publish to subTransaction
                try
                {
                  subTransaction.setPersistentMessageIncluded(isPersistent);
                  if (sub.isRemote())
                  {
                    if (ctx.traceSpace.enabled)
                      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", remote, therefore publish AS copy");
                    subTransaction.publish(copyMessage(message));
                  } else
                  {
                    if (ctx.traceSpace.enabled)
                      ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "publishToNode, sub=" + sub + ", publish NO copy");
                    subTransaction.publish(message);
                  }
                } catch (QueueTransactionClosedException qtce)
                {
                  transaction.setTopicSubscriberTransaction(sub.getBrokerSubscriberId(), null);
                } catch (Exception e)
                {
                  throw new QueueException(e.getMessage());
                }
              }
            }
          }
        }
      }
    }
  }

  private List getMatchedNodes(List matchedTopics)
  {
    if (matchedTopics == null)
      return null;
    List r = null;
    Iterator iter = subscriptions.entrySet().iterator();
    while (iter.hasNext())
    {
      PredicateNode node = (PredicateNode) ((Map.Entry) iter.next()).getValue();
      for (int i = 0; i < matchedTopics.size(); i++)
      {
        String[] tokenizedName = (String[]) matchedTopics.get(i);
        if (node.isMatch(tokenizedName))
        {
          if (r == null)
            r = new ArrayList();
          r.add(node);
          break;
        }
      }
    }
    return r;
  }

  private List getMatchedNodes(String[] publisherTopic)
  {
    List r = null;
    Iterator iter = subscriptions.entrySet().iterator();
    while (iter.hasNext())
    {
      PredicateNode node = (PredicateNode) ((Map.Entry) iter.next()).getValue();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "getMatchedNodes: '" + TopicManagerImpl.concatName(publisherTopic) + "' checks '" + node + "'");
      if (node.isMatch(publisherTopic))
      {
        if (r == null)
          r = new ArrayList();
        r.add(node);
      }
    }
    return r;
  }

  public void removeRemoteSubscriptions(String destination)
  {
    lockAndWaitAsyncFinished();
    try
    {
      Iterator iter = subscriptions.entrySet().iterator();
      while (iter.hasNext())
      {
        PredicateNode node = (PredicateNode) ((Map.Entry) iter.next()).getValue();
        node.removeRemoteSubscriptions(destination);
      }
    } finally
    {
      lock.unlock();
    }
  }

  public void putMessage(Object transactionId, MessageImpl msg)
      throws QueueException
  {
    lockAndWaitAsyncFinished();
    try
    {
      if (subscriptions.size() == 0)
        return;
      TopicTransaction transaction = (TopicTransaction) transactionId;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "putMessage, transaction=" + transaction + ", message=" + msg);
      // set dest queue for routing
      if (msg.getDestQueue() == null)
      {
        String s = getQueueName();
        msg.setDestQueue(s.substring(0, s.indexOf('@')));
      }
      String topicName = null;
      // local msgs haven't set a source router
      boolean publishedLocal = msg.getSourceRouter() == null;
      try
      {
        TopicImpl topic = (TopicImpl) msg.getJMSDestination();
        topicName = topic.getTopicName();
      } catch (Exception ignored)
      {
      }
      String[] tokenizedPubTopic = topicName.indexOf(TopicManagerImpl.TOPIC_DELIMITER_CHAR) == -1 ? rootTokenized : ctx.topicManager.tokenizeTopicName(topicName);
      List matchedNodes = null;
      if (ctx.topicManager.isDirectSubscriberSelection())
        matchedNodes = getMatchedNodes(tokenizedPubTopic);
      else
        matchedNodes = getMatchedNodes(getMatchedTopics(tokenizedPubTopic));
      if (matchedNodes != null)
      {
        for (int i = 0; i < matchedNodes.size(); i++)
        {
          PredicateNode node = (PredicateNode) matchedNodes.get(i);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "putMessage, node: " + node + " does match");
          publishToNode(transaction, msg, node, publishedLocal);
        }
      }
    } finally
    {
      lock.unlock();
    }
  }

  public String toString()
  {
    return tracePrefix;
  }

  private class TopicEntry
  {
    String topicName;
    String[] tokenizedName;

    TopicEntry(String topicName, String[] tokenizedName)
    {
      this.topicName = topicName;
      this.tokenizedName = tokenizedName;
    }

    String getTopicName()
    {
      return topicName;
    }

    String[] getTokenizedName()
    {
      return tokenizedName;
    }

    boolean isMatch(String[] tokenizedPredicate)
    {
      if (tokenizedName.length < tokenizedPredicate.length) // <, fix 3.2.0
        return false;
      boolean match = true;
      // must begin at pos 0 otherwise it does not match the root broker
      for (int i = 0; i < tokenizedPredicate.length; i++)
      {
        if (!LikeComparator.compare(tokenizedName[i], tokenizedPredicate[i], '\\'))
        {
          match = false;
          break;
        }
      }
      return match;
    }
  }

  private class PredicateNode
  {
    String topicName;
    String[] tokenizedPredicate;
    List subscribers = new ArrayList();
    int localCount = 0;

    PredicateNode(String topicName, String[] tokenizedPredicate)
    {
      this.topicName = topicName;
      this.tokenizedPredicate = tokenizedPredicate;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' created");
    }

    String getTopicName()
    {
      return topicName;
    }

    String[] getTokenizedPredicate()
    {
      return tokenizedPredicate;
    }

    void addSubscription(TopicSubscription subscriber)
    {
      subscribers.add(subscriber);
      if (!subscriber.isRemote())
        localCount++;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' adding subscriber: " + subscriber + ", count = " + subscribers.size());
    }

    void removeSubscription(TopicSubscription subscriber, boolean force)
    {
      if (!force && subscriber.isRemote() && subscriber.isStaticSubscription() && subscriber.isKeepOnUnsubscribe())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' must keep subscriber!");
        return;
      }
      subscribers.remove(subscriber);
      if (!subscriber.isRemote())
        localCount--;
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' removing subscriber: " + subscriber + ", count = " + subscribers.size());
    }

    void removeRemoteSubscriptions(String destination)
    {
      for (int i = subscribers.size() - 1; i >= 0; i--)
      {
        TopicSubscription subscriber = (TopicSubscription) subscribers.get(i);
        if (subscriber.isRemote() && subscriber.getDestination().equals(destination))
        {
          if (subscriber.isStaticSubscription() && subscriber.isKeepOnUnsubscribe())
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' must keep subscriber!");
          } else
            subscribers.remove(i);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "' removing remote subscriber: " + subscriber + ", count = " + subscribers.size());
        }
      }
    }

    TopicSubscription getSubscription(TopicSubscription subscriber)
    {
      for (int i = subscribers.size() - 1; i >= 0; i--)
      {
        TopicSubscription ts = (TopicSubscription) subscribers.get(i);
        if (subscriber.equals(ts))
          return ts;
      }
      return null;
    }

    boolean hasSubscription(TopicSubscription subscriber)
    {
      return subscribers.contains(subscriber);
    }

    boolean hasLocalSubscription()
    {
      for (int i = 0; i < subscribers.size(); i++)
      {
        TopicSubscription subscriber = (TopicSubscription) subscribers.get(i);
        if (!subscriber.isRemote())
          return true;
      }
      return false;
    }

    List getSubscriptions()
    {
      return subscribers;
    }

    int getSubscriptionCount()
    {
      return subscribers.size();
    }

    int getLocalSubscriptionCount()
    {
      return localCount;
    }

    boolean isMatch(String[] tokenizedName)
    {
      boolean match = true;
      for (int i = 1; i < tokenizedPredicate.length && i < tokenizedName.length; i++)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "', isMatch checks " + tokenizedName[i] + " with " + tokenizedPredicate[i]);
        if (!LikeComparator.compare(tokenizedName[i], tokenizedPredicate[i], '\\'))
        {
          match = false;
          break;
        }
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "', isMatch returns " + match);
      return match;
    }

    boolean isEmpty()
    {
      boolean empty = true;
      for (int i = 0; i < subscribers.size(); i++)
      {
        if (subscribers.get(i) != null)
        {
          empty = false;
          break;
        }
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "PredicateNode '" + toString() + "', isEmpty returns " + empty);
      return empty;
    }

    public String toString()
    {
      StringBuffer s = new StringBuffer();
      s.append("[PredicateNode '");
      s.append(topicName);
      s.append("']");
      return s.toString();
    }
  }

  private class NonDurableRemover implements TimerListener
  {
    String subscriberQueueName = null;

    public NonDurableRemover(String subscriberQueueName)
    {
      this.subscriberQueueName = subscriberQueueName;
    }

    private String getClientId()
    {
      String clientId = null;
      Map entities = ctx.activeSubscriberList.getEntities();
      if (entities != null && entities.size() > 0)
      {
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); )
        {
          Entity subEntity = (Entity) ((Map.Entry) iter.next()).getValue();
          if (subEntity.getProperty("boundto").getValue().equals(subscriberQueueName))
          {
            clientId = (String) subEntity.getProperty("clientid").getValue();
            break;
          }
        }
      }
      return clientId;
    }

    private Entity getJMSEntity(EntityList jmsUsageList, String clientId)
    {
      Map entities = jmsUsageList.getEntities();
      if (entities != null && entities.size() > 0)
      {
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); )
        {
          Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
          if (entity.getProperty("clientid").getValue().equals(clientId))
            return entity;
        }
      }
      return null;
    }

    public void performTimeAction()
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "NonDurableRemover '" + subscriberQueueName + "' ...");
      String clientId = getClientId();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "NonDurableRemover '" + subscriberQueueName + "', clientId=" + clientId);
      EntityList jmsUsageList = (EntityList) SwiftletManager.getInstance().getConfiguration("sys$jms").getEntity("usage");
      Entity jmsEntity = getJMSEntity(jmsUsageList, clientId);
      if (jmsEntity != null)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "NonDurableRemover '" + subscriberQueueName + "', jmsEntity=" + jmsEntity.getName());
        try
        {
          jmsUsageList.removeEntity(jmsEntity);
        } catch (EntityRemoveException e)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "NonDurableRemover '" + subscriberQueueName + "', exception=" + e);
        }
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "NonDurableRemover '" + subscriberQueueName + "' done");
    }
  }

  private class DurableRemover extends NonDurableRemover
  {

    public DurableRemover(String subscriberQueueName)
    {
      super(subscriberQueueName);
    }

    public void performTimeAction()
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "DurableRemover '" + subscriberQueueName + "' ...");
      super.performTimeAction();
      ctx.timerSwiftlet.addInstantTimerListener(1000, new DurableQueueDropper(subscriberQueueName));
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "DurableRemover '" + subscriberQueueName + "' done");
    }
  }

  private class DurableQueueDropper implements TimerListener
  {
    String subscriberQueueName = null;

    public DurableQueueDropper(String subscriberQueueName)
    {
      this.subscriberQueueName = subscriberQueueName;
    }

    private Entity getDurableEntity(EntityList durableUsageList)
    {
      Map entities = durableUsageList.getEntities();
      if (entities != null && entities.size() > 0)
      {
        for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); )
        {
          Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
          if (entity.getProperty("boundto").getValue().equals(subscriberQueueName))
            return entity;
        }
      }
      return null;
    }

    public void performTimeAction()
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "DurableQueueDropper '" + subscriberQueueName + "' ...");
      Entity durableEntity = getDurableEntity(ctx.activeDurableList);
      try
      {
        if (durableEntity != null)
          ctx.activeDurableList.removeEntity(durableEntity);
      } catch (EntityRemoveException e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "DurableQueueDropper '" + subscriberQueueName + "', exception=" + e);
      }
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "DurableQueueDropper '" + subscriberQueueName + "' done");
    }
  }

  private class TraceableBitSet extends BitSet
  {
    public TraceableBitSet(int size)
    {
      super(size);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "TraceableBitSet(" + size + ") created");
    }

    public void set(int i)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "TraceableBitSet.set(" + i + ")");
      super.set(i);
    }

    public void clear(int i)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "TraceableBitSet.clear(" + i + ")");
      super.clear(i);
    }

    public int nextClearBit(int i)
    {
      int idx = super.nextClearBit(i);
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace("sys$topicmanager", tracePrefix + "TraceableBitSet.nextClearBit(" + i + ")=" + idx);
      return idx;
    }
  }
}

