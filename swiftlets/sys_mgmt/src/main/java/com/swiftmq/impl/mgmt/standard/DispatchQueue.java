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

package com.swiftmq.impl.mgmt.standard;

import com.swiftmq.impl.mgmt.standard.po.*;
import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.*;
import com.swiftmq.mgmt.protocol.ProtocolFactory;
import com.swiftmq.mgmt.protocol.ProtocolReply;
import com.swiftmq.mgmt.protocol.ProtocolRequest;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.swiftlet.routing.event.RoutingEvent;
import com.swiftmq.swiftlet.routing.event.RoutingListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.dump.Dumpalizer;
import com.swiftmq.tools.pipeline.PipelineQueue;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import javax.jms.DeliveryMode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DispatchQueue
    implements EventVisitor, TimerListener, EntityWatchListener, PropertyWatchListener, RoutingListener
{
  static final String TP_DISPATCH = "sys$mgmt.dispatchqueue";
  static final long EXPIRATION_CHECK_INTERVAL = 60000;

  SwiftletContext ctx = null;
  PipelineQueue pipelineQueue = null;
  Map dispatchers = new HashMap();
  TimerListener updateTimer = null;
  boolean leaseStarted = false;
  ProtocolFactory factory = new ProtocolFactory();
  DataByteArrayInputStream dis = new DataByteArrayInputStream();
  DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
  long flushInterval = 0;

  public DispatchQueue(SwiftletContext ctx)
  {
    this.ctx = ctx;
    pipelineQueue = new PipelineQueue(ctx.threadpoolSwiftlet.getPool(TP_DISPATCH), TP_DISPATCH, this);
    ctx.usageList.setEntityRemoveListener(new EntityRemoveListener()
    {
      public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException
      {
        pipelineQueue.enqueue(new Disconnect(delEntity.getName()));
      }
    });
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/created");
  }

  // --> private methods
  private void addWatchListeners(Entity entity)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/addWatchListeners, entity: " + entity.getName());
    Map m = entity.getProperties();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); )
      {
        Property prop = (Property) ((Map.Entry) iter.next()).getValue();
        prop.addPropertyWatchListener(this);
      }
    }
    m = entity.getEntities();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); )
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        e.addEntityWatchListener(this);
        addWatchListeners(e);
      }
    }
  }

  private void removeWatchListeners(Entity entity)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/removeWatchListeners, entity: " + entity.getName());
    Map m = entity.getProperties();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); )
      {
        Property prop = (Property) ((Map.Entry) iter.next()).getValue();
        prop.removePropertyWatchListener(this);
      }
    }
    m = entity.getEntities();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext(); )
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        e.removeEntityWatchListener(this);
        removeWatchListeners(e);
      }
    }
  }

  private void checkStartLeases()
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkStartLeases, dispatchers.size(): " + dispatchers.size());
    if (dispatchers.size() == 1)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkStartLeases, start timers");
      ctx.timerSwiftlet.addTimerListener(EXPIRATION_CHECK_INTERVAL, this);
      updateTimer = new TimerListener()
      {
        public void performTimeAction()
        {
          pipelineQueue.enqueue(new SendUpdates());
        }
      };
      Property prop = ctx.root.getProperty("flush-interval");
      flushInterval = ((Long) prop.getValue()).longValue();
      ctx.timerSwiftlet.addTimerListener(flushInterval, updateTimer);
      prop.setPropertyChangeListener(new PropertyChangeListener()
      {
        public void propertyChanged(Property property, Object oldValue, Object newValue)
            throws PropertyChangeException
        {
          ctx.timerSwiftlet.removeTimerListener(updateTimer);
          flushInterval = ((Long) newValue).longValue();
          ctx.timerSwiftlet.addTimerListener(flushInterval, updateTimer);
        }
      });
      RouterConfiguration.Singleton().addEntityWatchListener(this);
      addWatchListeners(RouterConfiguration.Singleton());
      ctx.mgmtSwiftlet.fireEvent(true);
      leaseStarted = true;
    }
  }

  private void checkStopLeases()
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkStopLeases, dispatchers.size(): " + dispatchers.size());
    if (dispatchers.size() == 0 && leaseStarted)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkStopLeases, stop timers");
      ctx.timerSwiftlet.removeTimerListener(this);
      ctx.timerSwiftlet.removeTimerListener(updateTimer);
      updateTimer = null;
      RouterConfiguration.Singleton().removeEntityWatchListener(this);
      removeWatchListeners(RouterConfiguration.Singleton());
      ctx.mgmtSwiftlet.fireEvent(false);
      leaseStarted = false;
    }
  }

  private void checkExpire()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkExpire ...");
    for (Iterator iter = dispatchers.entrySet().iterator(); iter.hasNext(); )
    {
      Dispatcher d = (Dispatcher) ((Map.Entry) iter.next()).getValue();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkExpire, dispatcher: " + d);
      if (d.isExpired())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkExpire, dispatcher expired: " + d);
        d.doExpire();
        d.close();
        iter.remove();
      }
    }
    checkStopLeases();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/checkExpire done");
  }

  private void dispatch(EventObject event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + " ...");
    for (Iterator iter = dispatchers.entrySet().iterator(); iter.hasNext(); )
    {
      Dispatcher d = (Dispatcher) ((Map.Entry) iter.next()).getValue();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + ", dispatcher: " + d);
      if (d.isInvalid())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + ", dispatcher invalid (1): " + d);
        d.close();
        iter.remove();
      } else
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + ", dispatcher process: " + d);
        d.process(event);
        if (d.isInvalid())
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + ", dispatcher invalid (2): " + d);
          d.close();
          iter.remove();
        }
      }
    }
    checkStopLeases();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatch, event: " + event + " done");
  }

  private void dispatchClientRequest(ClientRequest event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " ...");
    Dispatcher d = (Dispatcher) dispatchers.get(event.getQueueName());
    if (d != null)
    {
      if (d.isInvalid())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + ", dispatcher invalid (1): " + d);
        d.close();
        dispatchers.remove(event.getQueueName());
      } else
      {
        d.process(event);
        if (d.isInvalid())
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + ", dispatcher invalid (2): " + d);
          d.close();
          dispatchers.remove(event.getQueueName());
        }
      }
    } else
    {
      try
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " try ProtocolRequest ...");
        dis.reset();
        dis.setBuffer(event.getBuffer());
        ProtocolRequest r = null;
        try
        {
          r = (ProtocolRequest) Dumpalizer.construct(dis, factory);
        } catch (NullPointerException e)
        {
          // Dumpalizer throws a NPE if it cannot construct the request from the factory.
          // Since we can get old LeaseRequests here, we can ignore it.
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, got exception: " + e + ", probably old LeaseRequest, ignore!");
          return;
        }
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " got ProtocolRequest: " + r);
        ProtocolReply reply = (ProtocolReply) r.createReply();
        switch (r.getVersion())
        {
          case 400:
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " create v400 Dispatcher");
            d = new com.swiftmq.impl.mgmt.standard.v400.DispatcherImpl(ctx, event.getQueueName());
            dispatchers.put(event.getQueueName(), d);
            reply.setOk(true);
            break;
          case 750:
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " create v750 Dispatcher");
            d = new com.swiftmq.impl.mgmt.standard.v750.DispatcherImpl(ctx, event.getUserName(), event.getQueueName());
            dispatchers.put(event.getQueueName(), d);
            reply.setOk(true);
            break;
          default:
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " failed: Unsupported protocol version: " + r.getVersion());
            reply.setOk(false);
            reply.setException(new Exception("Unsupported protocol version: " + r.getVersion()));
        }
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " send reply: " + reply);
        QueueSender sender = ctx.queueManager.createQueueSender(event.getQueueName(), null);
        BytesMessageImpl msg = new BytesMessageImpl();
        msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        msg.setJMSDestination(new QueueImpl(event.getQueueName()));
        msg.setJMSPriority(MessageImpl.MAX_PRIORITY - 1);
        dos.rewind();
        Dumpalizer.dump(dos, reply);
        msg.writeBytes(dos.getBuffer(), 0, dos.getCount());
        QueuePushTransaction t = sender.createTransaction();
        t.putMessage(msg);
        t.commit();
        sender.close();
        checkStartLeases();
      } catch (Exception e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " got exception: " + e);
        ctx.logSwiftlet.logError(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " got exception: " + e);
        dispatchers.remove(event.getQueueName());
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, event: " + event + " done");
  }

  private void disconnect(Disconnect event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/disconnect, event: " + event + " ...");
    Dispatcher d = (Dispatcher) dispatchers.get(event.getName());
    if (d != null)
    {
      if (d.isInvalid())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/disconnect, event: " + event + ", dispatcher invalid (1): " + d);
        d.close();
        dispatchers.remove(event.getName());
      } else
      {
        d.doDisconnect();
        d.close();
        dispatchers.remove(event.getName());
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/disconnect, event: " + event + " done");
  }

  private void flushAll()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll ...");
    for (Iterator iter = dispatchers.entrySet().iterator(); iter.hasNext(); )
    {
      Dispatcher d = (Dispatcher) ((Map.Entry) iter.next()).getValue();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll, dispatcher: " + d);
      if (d.isInvalid())
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll, dispatcher invalid (1): " + d);
        d.close();
        iter.remove();
      } else
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll, trigger dispatcher: " + d);
        d.flush();
        if (d.isInvalid())
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll, dispatcher invalid (2): " + d);
          d.close();
          iter.remove();
        }
      }
    }
    checkStopLeases();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/flushAll done");
  }
  // <-- private methods

  // --> EntityWatchListener
  public void entityAdded(Entity parent, Entity newEntity)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityAdded, entity: " + newEntity.getName() + " ...");
    addWatchListeners(newEntity);
    if (newEntity instanceof Configuration)
      pipelineQueue.enqueue(new SwiftletAdded(newEntity.getName(), (Configuration) newEntity));
    else
    {
      pipelineQueue.enqueue(new EntityAdded(parent.getContext(), newEntity.getName()));
      for (Iterator iter = newEntity.getProperties().entrySet().iterator(); iter.hasNext(); )
      {
        Property prop = (Property) ((Map.Entry) iter.next()).getValue();
        if (prop.getValue() != prop.getDefaultValue())
          propertyValueChanged(prop);
      }
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityAdded, entity: " + newEntity.getName() + " done");
  }

  public void entityRemoved(Entity parent, Entity delEntity)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityRemoved, entity: " + delEntity.getName() + " ...");
    removeWatchListeners(delEntity);
    if (delEntity instanceof Configuration)
      pipelineQueue.enqueue(new SwiftletRemoved(delEntity.getName()));
    else
      pipelineQueue.enqueue(new EntityRemoved(parent.getContext(), delEntity.getName()));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/entityRemoved, entity: " + delEntity.getName() + " done");
  }
  // <-- EntityWatchListener

  // --> PropertyWatchListener
  public void propertyValueChanged(Property prop)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/propertyValueChanged, prop: " + prop.getName() + " ...");
    String[] entityListContext = null;
    if (prop.getParent().getParent() != null && prop.getParent().getParent() instanceof EntityList)
      entityListContext = prop.getParent().getParent().getContext();
    pipelineQueue.enqueue(new PropertyChanged(entityListContext, prop.getParent().getContext(), prop.getName(), prop.getValue()));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/propertyValueChanged, prop: " + prop.getName() + " done");
  }
  // <-- PropertyWatchListener

  // --> RoutingListener
  public void destinationAdded(RoutingEvent event)
  {
    // do nothing
  }

  public void destinationRemoved(RoutingEvent event)
  {
    // do nothing
  }

  public void destinationActivated(RoutingEvent event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/destinationActivated, event: " + event + " ...");
    pipelineQueue.enqueue(new RouterAvailable(event.getDestination()));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/destinationActivated, event: " + event + " done");
  }

  public void destinationDeactivated(RoutingEvent event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/destinationDeactivated, event: " + event + " ...");
    pipelineQueue.enqueue(new RouterUnavailable(event.getDestination()));
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/destinationDeactivated, event: " + event + " done");
  }
  // <-- RoutingListener

  // --> TimerListener
  public void performTimeAction()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/performTimeAction ...");
    pipelineQueue.enqueue(new CheckExpire());
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/performTimeAction done");
  }
  // <-- TimerListener

  // --> Exposed Methods
  public void dispatchClientRequest(String userName, String queueName, byte[] buffer)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/dispatchClientRequest, userName=" + userName + ", queueName" + queueName + ", buffer.length: " + buffer.length);
    pipelineQueue.enqueue(new ClientRequest(userName, queueName, buffer));
  }
  // <-- Exposed Methods

  // --> EventVisitor methods
  public void visit(EntityAdded event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(EntityRemoved event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(SwiftletAdded event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(SwiftletRemoved event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(PropertyChanged event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(ClientRequest event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatchClientRequest(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(RouterAvailable event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(RouterUnavailable event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    dispatch(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(CheckExpire event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    checkExpire();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(Disconnect event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    disconnect(event);
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(SendUpdates event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    flushAll();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }

  public void visit(Close event)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " ...");
    ctx.timerSwiftlet.removeTimerListener(this);
    if (updateTimer != null)
      ctx.timerSwiftlet.removeTimerListener(updateTimer);
    for (Iterator iter = dispatchers.entrySet().iterator(); iter.hasNext(); )
    {
      Dispatcher d = (Dispatcher) ((Map.Entry) iter.next()).getValue();
      d.close();
    }
    dispatchers.clear();
    pipelineQueue.close();
    if (event.getSemaphore() != null)
      event.getSemaphore().notifySingleWaiter();
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/visit, event" + event + " done");
  }
  // <-- EventVisitor methods

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/close ...");
    Semaphore sem = new Semaphore();
    pipelineQueue.enqueue(new Close(sem));
    sem.waitHere();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.mgmtSwiftlet.getName(), toString() + "/close done");
  }

  public String toString()
  {
    return "DispatchQueue";
  }
}
