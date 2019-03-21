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

package com.swiftmq.impl.xa.standard;

import com.swiftmq.jms.BytesMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityChangeAdapter;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.mgmt.EntityWatchListener;
import com.swiftmq.ms.MessageSelector;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.MessageEntry;
import com.swiftmq.swiftlet.queue.QueuePullTransaction;
import com.swiftmq.swiftlet.queue.QueuePushTransaction;
import com.swiftmq.swiftlet.queue.QueueReceiver;
import com.swiftmq.swiftlet.queue.QueueSender;
import com.swiftmq.tools.util.DataByteArrayInputStream;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.util.IdGenerator;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class HeuristicHandler extends EntityChangeAdapter implements EntityWatchListener
{
  private static final String HEURISTIC_QUEUE = "sys$xa_heuristic";
  private static final String PROP_SID = "sid";
  private static final String PROP_IID = "iid";
  private static final String PROP_OPER = "operation";
  private static final String MSGID = "sys$xa_heuristic/";
  SwiftletContext ctx = null;
  volatile boolean duringLoad = false;
  volatile int maxId = 0;

  public HeuristicHandler(SwiftletContext ctx)
  {
    super(null);
    this.ctx = ctx;
    ctx.heuristicUsageList.setEntityRemoveListener(this);
    ctx.heuristicUsageList.addEntityWatchListener(this);    
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/created");
  }

  private void messageToEntity(MessageImpl message, Entity entity) throws Exception
  {
    entity.setName(message.getStringProperty(PROP_SID));
    BytesMessageImpl msg = (BytesMessageImpl) message;
    byte[] b = new byte[(int) msg.getBodyLength()];
    msg.readBytes(b);
    DataByteArrayInputStream dis = new DataByteArrayInputStream(b);
    XidImpl xid = new XidImpl();
    xid.readContent(dis);
    entity.setUserObject(xid);
    entity.getProperty("xid").setValue(xid.toString());
    entity.getProperty("operation").setValue(msg.getStringProperty(PROP_OPER));
  }

  private MessageImpl entityToMessage(Entity entity) throws Exception
  {
    XidImpl xid = (XidImpl) entity.getUserObject();
    DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
    xid.writeContent(dos);
    BytesMessageImpl message = new BytesMessageImpl();
    message.setJMSDestination(new QueueImpl(HEURISTIC_QUEUE));
    message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
    message.setJMSPriority(Message.DEFAULT_PRIORITY);
    message.setJMSExpiration(Message.DEFAULT_TIME_TO_LIVE);
    message.setJMSMessageID(MSGID + IdGenerator.getInstance().nextId('/'));
    message.setStringProperty(PROP_OPER, (String) entity.getProperty(PROP_OPER).getValue());
    message.setStringProperty(PROP_SID, entity.getName());
    message.setIntProperty(PROP_IID, Integer.parseInt(entity.getName()));
    message.writeBytes(dos.getBuffer(), 0, dos.getCount());
    return message;
  }

  private void removeHeuristic(String id) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/removeHeuristic, id="+id);
    MessageSelector selector = new MessageSelector(PROP_SID + " = '" + id + "'");
    selector.compile();
    QueueReceiver receiver = ctx.queueManager.createQueueReceiver(HEURISTIC_QUEUE, null, selector);
    QueuePullTransaction t = receiver.createTransaction(false);
    MessageEntry entry = t.getMessage(0, selector);
    t.commit();
    receiver.close();
    if (entry == null)
      throw new Exception("Heuristic with ID '" + id + "' not found!");
  }

  private void storeHeuristic(MessageImpl msg) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/storeHeuristic, msg="+msg);
    QueueSender sender = ctx.queueManager.createQueueSender(HEURISTIC_QUEUE, null);
    QueuePushTransaction t = sender.createTransaction();
    t.putMessage(msg);
    t.commit();
    sender.close();
  }

  private Entity findHeuristic(XidImpl xid)
  {
    Map map = ctx.heuristicUsageList.getEntities();
    if (map != null && map.size() > 0)
    {
      for (Iterator iter = map.entrySet().iterator(); iter.hasNext();)
      {
        Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
        XidImpl thatXid = (XidImpl) entity.getUserObject();
        if (thatXid.equals(xid))
          return entity;
      }
    }
    return null;
  }

  public void removeHeuristic(XidImpl xid) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/removeHeuristic, xid="+xid);
    Entity entity = findHeuristic(xid);
    if (entity != null)
      ctx.heuristicUsageList.removeEntity(entity);
  }

  public boolean hasHeuristic(XidImpl xid)
  {
    return findHeuristic(xid) != null;
  }

  public boolean wasCommit(XidImpl xid)
  {
    Entity entity = findHeuristic(xid);
    if (entity != null)
      return entity.getProperty("operation").getValue().equals("COMMIT");
    return false;
  }

  public List getXids()
  {
    Map map = ctx.heuristicUsageList.getEntities();
    if (map != null && map.size() > 0)
    {
      List list = new ArrayList();
      for (Iterator iter = map.entrySet().iterator(); iter.hasNext();)
      {
        Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
        list.add(entity.getUserObject());
      }
      return list;
    } else
    return null;
  }

  public void addHeuristic(XidImpl xid, boolean commit) throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/addHeuristic, xid="+xid);
    Entity entity = ctx.heuristicUsageList.createEntity();
    if (maxId == Integer.MAX_VALUE)
      maxId = 0;
    entity.setName(String.valueOf(++maxId));
    entity.setUserObject(xid);
    entity.getProperty("xid").setValue(xid.toString());
    entity.getProperty("operation").setValue(commit ? "COMMIT" : "ROLLBACK");
    entity.createCommands();
    ctx.heuristicUsageList.addEntity(entity);
  }

  public void loadHeuristics() throws Exception
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/loadHeuristics");
    duringLoad = true;
    try
    {
      if (!ctx.queueManager.isQueueDefined(HEURISTIC_QUEUE))
        ctx.queueManager.createQueue(HEURISTIC_QUEUE, (ActiveLogin) null);
      QueueReceiver receiver = ctx.queueManager.createQueueReceiver(HEURISTIC_QUEUE, null, null);
      QueuePullTransaction t = receiver.createTransaction(false);
      MessageEntry entry = null;
      while ((entry = t.getMessage(0)) != null)
      {
        Entity entity = ctx.heuristicUsageList.createEntity();
        MessageImpl msg = entry.getMessage();
        maxId = Math.max(maxId, msg.getIntProperty(PROP_IID));
        messageToEntity(msg, entity);
        entity.createCommands();
        ctx.heuristicUsageList.addEntity(entity);
      }
      t.rollback();
      receiver.close();
    } finally
    {
      duringLoad = false;
    }
  }

  public void entityAdded(Entity parent, Entity newEntity)
  {
    if (duringLoad)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/entityAdded");
    try
    {
      storeHeuristic(entityToMessage(newEntity));
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/entityAdded, exception="+e);
    }
  }

  public void entityRemoved(Entity entity, Entity entity1)
  {
    // do nothing
  }

  public void onEntityRemove(Entity parent, Entity oldEntity) throws EntityRemoveException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/onEntityRemove");
    try
    {
      removeHeuristic(oldEntity.getName());
    } catch (Exception e)
    {
      throw new EntityRemoveException(e.getMessage());
    }
  }

  public void close()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.xaSwiftlet.getName(), toString() + "/close");
    ctx.heuristicUsageList.setEntityRemoveListener(null);
    ctx.heuristicUsageList.removeEntityWatchListener(this);
  }

  public String toString()
  {
    return "[HeuristicHandler]";
  }
}
