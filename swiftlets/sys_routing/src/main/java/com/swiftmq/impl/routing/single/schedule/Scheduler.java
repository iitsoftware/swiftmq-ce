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

package com.swiftmq.impl.routing.single.schedule;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.route.Route;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.schedule.po.*;
import com.swiftmq.impl.routing.single.smqpr.*;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.pipeline.PipelineQueue;

public abstract class Scheduler
  implements POSchedulerVisitor, DeliveryCallback
{
  static final String TP_SCHEDULER = "sys$routing.scheduler";

  SwiftletContext ctx = null;
  String destinationRouter = null;
  String queueName = null;
  PipelineQueue pipelineQueue = null;
  QueueReceiver receiver = null;
  QueuePullTransaction readTransaction = null;
  RoutingConnection currentConnection = null;
  DeliveryRequest deliveryRequest = null;
  PODeliverObject deliverObject = null;
  PODeliveredObject deliveredObject = null;
  MP mp = null;
  boolean processorActive = false;
  boolean deliveryActive = false;
  boolean closed = false;

  public Scheduler(SwiftletContext ctx, String destinationRouter, String queueName)
  {
    this.ctx = ctx;
    this.destinationRouter = destinationRouter;
    this.queueName = queueName;
    mp = new MP();
    pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_SCHEDULER), TP_SCHEDULER, this);
  }

  public String getQueueName()
  {
    return queueName;
  }

  public abstract void addRoute(Route route);

  public abstract void removeRoute(Route route);

  public abstract void removeRoutingConnection(RoutingConnection routingConnection);

  public abstract int getNumberConnections();

  protected abstract RoutingConnection getNextConnection();

  public abstract void close();

  private void startProcessor() throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/startProcessor ...");
    processorActive = false;
    currentConnection = getNextConnection();
    if (currentConnection == null)
    {
      try
      {
        if (readTransaction != null)
          readTransaction.rollback();
        readTransaction = null;
      } catch (Exception e)
      {
      }
      try
      {
        if (receiver != null)
          receiver.close();
        receiver = null;
      } catch (Exception e)
      {
      }
      return;
    }
    int txSize = currentConnection.getTransactionSize();
    MessageEntry[] buffer = mp.getBulkBuffer();
    if (buffer == null || buffer.length < txSize)
      mp.createBuffer(txSize);
    if (receiver == null)
    {
      receiver = ctx.queueManager.createQueueReceiver(queueName, null, null);
    }
    if (readTransaction == null)
    {
      readTransaction = receiver.createTransaction(false);
    }
    readTransaction.registerMessageProcessor(mp);
    processorActive = true;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/startProcessor done");
  }

  private void schedule(int nMessages)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/schedule, nMessages=" + nMessages);
    if (deliveryRequest == null)
      deliveryRequest = new DeliveryRequest(destinationRouter, receiver, readTransaction, mp.getBulkBuffer(), nMessages, this);
    else
    {
      deliveryRequest.receiver = receiver;
      deliveryRequest.readTransaction = readTransaction;
      deliveryRequest.entries = mp.getBulkBuffer();
      deliveryRequest.len = nMessages;
    }
    if (deliverObject == null)
      deliverObject = new PODeliverObject(nMessages);
    pipelineQueue.enqueue(deliverObject);
  }

  public void delivered(DeliveryRequest deliveryRequest)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/delivered, deliveryRequest=" + deliveryRequest);
    if (deliveredObject == null)
      deliveredObject = new PODeliveredObject(deliveryRequest);
    pipelineQueue.enqueue(deliveredObject);
  }

  protected void connectionAdded(RoutingConnection connection)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/connectionAdded, connection=" + connection);
    pipelineQueue.enqueue(new POConnectionAddedObject(connection));
  }

  protected void connectionRemoved(RoutingConnection connection)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/connectionRemoved, connection=" + connection);
    pipelineQueue.enqueue(new POConnectionRemovedObject(connection));
  }

  protected void enqueueClose(POCloseObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/enqueueClose, po=" + po);
    pipelineQueue.enqueue(po);
  }

  public void visit(PODeliverObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
    if (currentConnection != null)
    {
      try
      {
        currentConnection.enqueueRequest(deliveryRequest);
        deliveryActive = true;
      } catch (Exception e)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e);
        try
        {
          if (readTransaction != null)
            readTransaction.rollback();
        } catch (Exception e1)
        {
          if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e1);
        }
        readTransaction = null;
      }
    } else
    {
      try
      {
        if (readTransaction != null)
          readTransaction.rollback();
      } catch (Exception e1)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e1);
      }
      readTransaction = null;
    }
    if (readTransaction == null)
    {
      try
      {
        startProcessor();
      } catch (Exception e)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e);
      }
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
  }

  public void visit(PODeliveredObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
    try
    {
      deliveryActive = false;
      startProcessor();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e);
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
  }

  public void visit(POConnectionAddedObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
    try
    {
      if (!processorActive && !deliveryActive)
        startProcessor();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e);
      try
      {
        if (readTransaction != null)
          readTransaction.rollback();
      } catch (Exception e1)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e1);
      }
      readTransaction = null;
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
  }

  public void visit(POConnectionRemovedObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
    if (currentConnection != null && po.getConnection() == currentConnection && deliveryActive)
    {
      try
      {
        if (readTransaction != null)
          readTransaction.rollback();
      } catch (Exception e1)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e1);
      }
      readTransaction = null;
      currentConnection = null;
      deliveryActive = false;
      try
      {
        startProcessor();
      } catch (Exception e)
      {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + ", exception=" + e);
      }
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
  }

  public void visit(POCloseObject po)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " ...");
    pipelineQueue.close();
    try
    {
      if (processorActive)
        readTransaction.unregisterMessageProcessor(mp);
    } catch (Exception e)
    {
    }
    try
    {
      if (readTransaction != null)
        readTransaction.rollback();
    } catch (Exception e)
    {
    }
    try
    {
      if (receiver != null)
        receiver.close();
    } catch (Exception e)
    {
    }
    closed = true;
    if (po.getCallback() != null)
      po.getCallback().onSuccess(po);
    if (po.getSemaphore() != null)
      po.getSemaphore().notifySingleWaiter();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), toString() + "/visit, po=" + po + " done");
  }

  public String toString()
  {
    return "[Scheduler, queueName=" + queueName + ", current=" + currentConnection + ", processorActive=" + processorActive + ", deliveryActive=" + deliveryActive + "]";
  }

  private class MP extends MessageProcessor
  {
    public MP()
    {
      setBulkMode(true);
      setAutoCommit(false);
    }

    public void createBuffer(int size)
    {
      createBulkBuffer(size);
    }

    public boolean isValid()
    {
      return !closed;
    }

    public void processMessage(MessageEntry entry)
    {
      // empty due to bulk mode
    }

    public void processException(Exception e)
    {
    }

    public void processMessages(int n)
    {
      schedule(n);
    }
  }
}
