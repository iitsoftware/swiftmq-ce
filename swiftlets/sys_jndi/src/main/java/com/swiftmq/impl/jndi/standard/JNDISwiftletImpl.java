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

package com.swiftmq.impl.jndi.standard;

import com.swiftmq.jms.DestinationFactory;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.mgmt.Configuration;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityChangeAdapter;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.mgmt.Property;
import com.swiftmq.mgmt.PropertyChangeAdapter;
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import com.swiftmq.tools.versioning.Versionable;
import com.swiftmq.tools.versioning.Versioned;
import com.swiftmq.util.SwiftUtilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class JNDISwiftletImpl extends JNDISwiftlet
{
  static final String TP_LISTENER = "sys$jndi.listener";
  static final int[] VERSIONS = {400};

  SwiftletContext ctx = null;

  Map aliases = Collections.synchronizedMap(new HashMap());
  Map objects = Collections.synchronizedMap(new HashMap());

  QueueJNDIProcessor queueJNDIProcessor = null;
  TopicJNDIProcessor topicJNDIProcessor = null;
  HashMap replications = new HashMap();

  private String findAlias(String name)
  {
    String alias = null;
    for (Iterator iter = aliases.keySet().iterator(); iter.hasNext();)
    {
      String key = (String) iter.next();
      String value = (String) aliases.get(key);
      if (value.equals(name))
      {
        alias = key;
        break;
      }
    }
    return alias;
  }

  private String[] findAliasesForObject(String name)
  {
    ArrayList al = new ArrayList();
    for (Iterator iter = aliases.keySet().iterator(); iter.hasNext();)
    {
      String key = (String) iter.next();
      String value = (String) aliases.get(key);
      if (value.equals(name))
      {
        al.add(key);
      }
    }
    return (String[]) al.toArray(new String[al.size()]);
  }

  public synchronized void registerJNDIObject(String name, Serializable object)
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(getName(), "registerJNDIObject, name=" + name + ", object=" + object);
    objects.put(name, object);

    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(getName(), "registerJNDIObject, registering on external JNDI server");
    try
    {
      bindReplications(name, object);
      String[] aliases = findAliasesForObject(name);
      if (aliases != null && aliases.length > 0)
      {
        for (int i = 0; i < aliases.length; i++)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "registerJNDIObject, registering on external JNDI server, alias=" + aliases[i]);
          bindReplications(aliases[i], object);
        }
      }
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(getName(), "registerJNDIObject, registering on external JNDI server, exception=" + e);
      ctx.logSwiftlet.logError(getName(), "registerJNDIObject, registering on external JNDI server, exception=" + e);
    }

    try
    {
      Entity entity = ctx.usageList.createEntity();
      entity.setName(name);
      Property prop = entity.getProperty("classname");
      prop.setValue(object.getClass().getName());
      prop.setReadOnly(true);
      entity.createCommands();
      ctx.usageList.addEntity(entity);
    } catch (Exception e)
    {
    }
  }

  public synchronized void deregisterJNDIObject(String name)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "deregisterJNDIObject, name=" + name);
    objects.remove(name);

    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(getName(), "deregisterJNDIObject, deregistering from external JNDI server");
    try
    {
      unbindReplications(name);
      String[] aliases = findAliasesForObject(name);
      if (aliases != null && aliases.length > 0)
      {
        for (int i = 0; i < aliases.length; i++)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "deregisterJNDIObject, deregistering on external JNDI server, alias=" + aliases[i]);
          unbindReplications(aliases[i]);
        }
      }
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(getName(), "deregisterJNDIObject, deregistering from external JNDI server, exception=" + e);
      ctx.logSwiftlet.logError(getName(), "deregisterJNDIObject, deregistering from external JNDI server, exception=" + e);
    }
    try
    {
      ctx.usageList.removeEntity(ctx.usageList.getEntity(name));
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public synchronized void deregisterJNDIQueueObject(String queueName)
  {
    try
    {
      for (Iterator iter = objects.keySet().iterator(); iter.hasNext();)
      {
        String key = (String) iter.next();
        Object registered = objects.get(key);
        if (registered instanceof QueueImpl)
        {
          String regQueueName = ((QueueImpl) registered).getQueueName();
          if (regQueueName != null && regQueueName.equals(queueName))
          {
            deregisterJNDIObject(key);
            break;
          }
        }
      }
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public synchronized void deregisterJNDIObjects(Comparable comparable)
  {
    try
    {
      List keys = new ArrayList();
      for (Iterator iter = objects.entrySet().iterator(); iter.hasNext();)
      {
        Map.Entry entry = (Map.Entry) iter.next();
        String key = (String) entry.getKey();
        Object registered = entry.getValue();
        if (comparable.compareTo(registered) == 0)
          keys.add(key);
      }
      for (int i = 0; i < keys.size(); i++)
        deregisterJNDIObject((String) keys.get(i));
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public synchronized Serializable getJNDIObject(String name)
  {
    String alias = (String) aliases.get(name);
    if (alias != null)
      name = alias;
    return (Serializable) objects.get(name);
  }

  public synchronized String getJNDIObjectName(QueueImpl queue)
  {
    String name = null;
    try
    {
      for (Iterator iter = objects.keySet().iterator(); iter.hasNext();)
      {
        String key = (String) iter.next();
        Object registered = objects.get(key);
        if (registered instanceof QueueImpl)
        {
          String queueName = ((QueueImpl) registered).getQueueName();
          if (queueName != null && queueName.equals(queue.getQueueName()))
          {
            name = key;
            break;
          }
        }
      }
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    return name;
  }

  synchronized void replicate(JNDIReplication replication)
  {
    for (Iterator iter = objects.keySet().iterator(); iter.hasNext();)
    {
      String key = (String) iter.next();
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "replicate: name=" + key + " to " + replication);
      Object value = objects.get(key);
      replication.bind(key, value);
      String[] aliases = findAliasesForObject(key);
      if (aliases != null && aliases.length > 0)
      {
        for (int i = 0; i < aliases.length; i++)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "replicate: name=" + aliases[i] + " to " + replication);
          replication.bind(aliases[i], value);
        }
      }
    }
  }

  private synchronized void bindReplications(String name, Object object)
  {
    for (Iterator iter = replications.entrySet().iterator(); iter.hasNext();)
    {
      JNDIReplication replication = (JNDIReplication) ((Map.Entry) iter.next()).getValue();
      if (replication.isEnabled() && replication.isConnected())
        replication.bind(name, object);
    }
  }

  private synchronized void unbindReplications(String name)
  {
    for (Iterator iter = replications.entrySet().iterator(); iter.hasNext();)
    {
      JNDIReplication replication = (JNDIReplication) ((Map.Entry) iter.next()).getValue();
      if (replication.isEnabled() && replication.isConnected())
        replication.unbind(name);
    }
  }

  private synchronized void createReplication(Entity replicationEntity)
  {
    String name = replicationEntity.getName();
    Property propEnabled = replicationEntity.getProperty("enabled");
    Property propKeepaliveInterval = replicationEntity.getProperty("keepalive-interval");
    Property propKeepaliveName = replicationEntity.getProperty("keepalive-lookup-name");
    Property propDestinationContext = replicationEntity.getProperty("destination-context");
    Property propNamePrefix = replicationEntity.getProperty("name-prefix");
    JNDIReplication replication = new JNDIReplication(ctx, name, ((Boolean) propEnabled.getValue()).booleanValue(),
        ((Long) propKeepaliveInterval.getValue()).longValue(),
        (String) propKeepaliveName.getValue(),
        (String) propDestinationContext.getValue(), (String) propNamePrefix.getValue(),
        (EntityList) replicationEntity.getEntity("environment-properties"));
    replications.put(name, replication);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createReplication: " + replication);

    propEnabled.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (enabled): name=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication myReplication = (JNDIReplication) replications.get(myName);
          myReplication.setEnabled(((Boolean) newValue).booleanValue());
        }
      }
    });

    propKeepaliveInterval.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (keepalive-interval): name=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication myReplication = (JNDIReplication) replications.get(myName);
          myReplication.setKeepaliveInterval(((Long) newValue).longValue());
        }
      }
    });

    propKeepaliveName.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (keepalive-name): name=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication myReplication = (JNDIReplication) replications.get(myName);
          myReplication.setKeepaliveName((String) newValue);
        }
      }
    });

    propDestinationContext.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (destination-context): name=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication myReplication = (JNDIReplication) replications.get(myName);
          myReplication.setDestinationContext((String) newValue);
          if (myReplication.isEnabled())
          {
            if (myReplication.isConnected())
              myReplication.disconnect();
            myReplication.connect();
          }
        }
      }
    });

    propNamePrefix.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (name-prefix): name=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication myReplication = (JNDIReplication) replications.get(myName);
          myReplication.setNamePrefix((String) newValue);
          if (myReplication.isEnabled())
          {
            if (myReplication.isConnected())
              myReplication.disconnect();
            myReplication.connect();
          }
        }
      }
    });
  }

  private void createReplications(EntityList replicationList)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "create replications ...");
    String[] s = replicationList.getEntityNames();
    if (s != null)
    {
      for (int i = 0; i < s.length; i++)
      {
        createReplication(replicationList.getEntity(s[i]));
      }
    } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "no replications defined");

    replicationList.setEntityAddListener(new EntityChangeAdapter(null)
    {
      public void onEntityAdd(Entity parent, Entity newEntity)
          throws EntityAddException
      {
        String name = newEntity.getName();
        createReplication(newEntity);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityAdd (replication): replication=" + name);
      }
    });
    replicationList.setEntityRemoveListener(new EntityChangeAdapter(null)
    {
      public void onEntityRemove(Entity parent, Entity delEntity)
          throws EntityRemoveException
      {
        String name = delEntity.getName();
        synchronized (JNDISwiftletImpl.this)
        {
          JNDIReplication replication = (JNDIReplication) replications.remove(name);
          replication.close();
        }
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "onEntityRemove (replication): replication=" + name);
      }
    });
  }

  private synchronized void createAlias(Entity aliasEntity)
  {
    String name = aliasEntity.getName();
    Property prop = aliasEntity.getProperty("map-to");
    String mapto = (String) prop.getValue();
    aliases.put(name, mapto);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "alias: map '" + name + "' to '" + mapto + "'");

    Object object = getJNDIObject(mapto);
    if (object != null)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createAlias, registering on external JNDI server");
      try
      {
        bindReplications(mapto, object);
      } catch (Exception e)
      {
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "createAlias, registering on external JNDI server, exception=" + e);
        ctx.logSwiftlet.logError(getName(), "createAlias, registering on external JNDI server, exception=" + e);
      }
    }

    prop.setPropertyChangeListener(new PropertyChangeAdapter(name)
    {
      public void propertyChanged(Property property, Object oldValue, Object newValue)
          throws PropertyChangeException
      {
        String myName = (String) configObject;
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace(getName(), "propertyChanged (alias): alias=" + myName + ", oldValue=" + oldValue + ", newValue=" + newValue);
        synchronized (JNDISwiftletImpl.this)
        {
          aliases.put(myName, newValue);
          if (oldValue != null)
          {
            if (ctx.traceSpace.enabled)
              ctx.traceSpace.trace(getName(), "propertyChanged (alias): deregistering from external JNDI server");
            try
            {
              unbindReplications(myName);
            } catch (Exception ignored)
            {
            }
            Object o = getJNDIObject(myName);
            if (o != null)
            {
              if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "propertyChanged (alias): registering on external JNDI server");
              try
              {
                bindReplications(myName, o);
              } catch (Exception ignored)
              {
              }
            }
          }
        }
      }
    });

  }

  private void createAliases(EntityList aliasList)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "create aliases ...");
    String[] s = aliasList.getEntityNames();
    if (s != null)
    {
      for (int i = 0; i < s.length; i++)
      {
        createAlias(aliasList.getEntity(s[i]));
      }
    } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "no aliases defined");

    aliasList.setEntityAddListener(new EntityChangeAdapter(null)
    {
      public void onEntityAdd(Entity parent, Entity newEntity)
          throws EntityAddException
      {
        String name = newEntity.getName();
        createAlias(newEntity);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityAdd (aliases): alias=" + name);
      }
    });
    aliasList.setEntityRemoveListener(new EntityChangeAdapter(null)
    {
      public void onEntityRemove(Entity parent, Entity delEntity)
          throws EntityRemoveException
      {
        String name = delEntity.getName();
        synchronized (JNDISwiftletImpl.this)
        {
          aliases.remove(name);
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "onEntityRemove (alias): deregistering from external JNDI server");
          try
          {
            unbindReplications(name);
          } catch (Exception ignored)
          {
          }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityRemove (aliases): alias=" + name);
      }
    });
  }

  public QueueImpl getStaticQueue(String name) throws Exception
  {
    StringTokenizer t = new StringTokenizer(name, "@");
    if (t.countTokens() != 2)
      throw new Exception("Invalid Queue Name, please specify <queue>@<router>!");
    SwiftUtilities.verifyQueueName(t.nextToken());
    SwiftUtilities.verifyRouterName(t.nextToken());
    return new QueueImpl(name);
  }

  private void createStatics(EntityList staticList)
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "create static objects ...");
    String[] s = staticList.getEntityNames();
    if (s != null)
    {
      for (int i = 0; i < s.length; i++)
      {
        try
        {
          DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
          DestinationFactory.dumpDestination(getStaticQueue(s[i]), dos);
          Versionable versionable = new Versionable();
          versionable.addVersioned(-1, new Versioned(-1, dos.getBuffer(), dos.getCount()), "com.swiftmq.jms.DestinationFactory");
          registerJNDIObject(s[i], versionable);
        } catch (Exception e)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "create static objects for '" + s[i] + "', exception=" + e);
        }
      }
    } else if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "no static objects defined");

    staticList.setEntityAddListener(new EntityChangeAdapter(null)
    {
      public void onEntityAdd(Entity parent, Entity newEntity)
          throws EntityAddException
      {
        String name = newEntity.getName();
        try
        {
          DataByteArrayOutputStream dos = new DataByteArrayOutputStream();
          DestinationFactory.dumpDestination(getStaticQueue(name), dos);
          Versionable versionable = new Versionable();
          versionable.addVersioned(-1, new Versioned(-1, dos.getBuffer(), dos.getCount()), "com.swiftmq.jms.DestinationFactory");
          registerJNDIObject(name, versionable);
        } catch (Exception e)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "onEntityAdd (statics) for '" + name + "', exception=" + e);
          throw new EntityAddException(e.getMessage());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityAdd (statics): static object=" + name);
      }
    });
    staticList.setEntityRemoveListener(new EntityChangeAdapter(null)
    {
      public void onEntityRemove(Entity parent, Entity delEntity)
          throws EntityRemoveException
      {
        String name = delEntity.getName();
        deregisterJNDIObject(name);
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "onEntityRemove (statics): static=" + name);
      }
    });
  }

  private void bindExternal()
  {
    for (Iterator iter = objects.keySet().iterator(); iter.hasNext();)
    {
      String key = (String) iter.next();
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(getName(), "bindExternal: registering generic name on external JNDI server, name=" + key);
      Object value = objects.get(key);
      bindReplications(key, value);
      String[] aliases = findAliasesForObject(key);
      if (aliases != null && aliases.length > 0)
      {
        for (int i = 0; i < aliases.length; i++)
        {
          if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "bindExternal, registering on external JNDI server, alias=" + aliases[i]);
          bindReplications(aliases[i], value);
        }
      }
    }
  }

  private void unbindExternal()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "unbinding aliases from external JNDI server");
    for (Iterator iter = aliases.keySet().iterator(); iter.hasNext();)
    {
      unbindReplications((String) iter.next());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "unbinding generic names from external JNDI server");
    for (Iterator iter = objects.keySet().iterator(); iter.hasNext();)
    {
      unbindReplications((String) iter.next());
    }
  }

  /**
   * Startup the swiftlet. Check if all required properties are defined and all other
   * startup conditions are met. Do startup work (i. e. start working thread, get/open resources).
   * If any condition prevends from startup fire a SwiftletException.
   *
   * @throws SwiftletException to prevend from startup
   */
  protected void startup(Configuration config)
      throws SwiftletException
  {
    ctx = new SwiftletContext();
    ctx.config = config;
    ctx.root = config;
    ctx.usageList = (EntityList) ctx.root.getEntity("usage");
    ctx.logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
    ctx.traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    ctx.traceSpace = ctx.traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);

    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");

    ctx.timerSwiftlet = (TimerSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$timer");
    ctx.threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");
    ctx.myTP = ctx.threadpoolSwiftlet.getPool(TP_LISTENER);
    ctx.queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
    ctx.topicManager = (TopicManager) SwiftletManager.getInstance().getSwiftlet("sys$topicmanager");
    ctx.jndiSwiftlet = this;
    createStatics((EntityList) config.getEntity("remote-queues"));
    createAliases((EntityList) config.getEntity("aliases"));
    createReplications((EntityList) config.getEntity("jndi-replications"));
    bindExternal();

    try
    {
      if (!ctx.queueManager.isQueueDefined(JNDISwiftlet.JNDI_QUEUE))
        ctx.queueManager.createQueue(JNDISwiftlet.JNDI_QUEUE, (ActiveLogin) null);
      if (!ctx.topicManager.isTopicDefined(JNDISwiftlet.JNDI_TOPIC))
        ctx.topicManager.createTopic(JNDISwiftlet.JNDI_TOPIC);

      queueJNDIProcessor = new QueueJNDIProcessor(ctx);
      topicJNDIProcessor = new TopicJNDIProcessor(ctx);
    } catch (Exception e)
    {
      throw new SwiftletException(e.getMessage());
    }
  }

  protected void shutdown()
      throws SwiftletException
  {
    // true when shutdown while standby
    if (ctx == null)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
    unbindExternal();
    for (Iterator iter = replications.entrySet().iterator(); iter.hasNext();)
    {
      ((JNDIReplication) ((Map.Entry) iter.next()).getValue()).close();
    }
    try
    {
      queueJNDIProcessor.close();
    } catch (Exception ignored)
    {
    }
    try
    {
      topicJNDIProcessor.close();
    } catch (Exception ignored)
    {
    }
    aliases.clear();
    objects.clear();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown: done.");
    ctx = null;
  }
}

