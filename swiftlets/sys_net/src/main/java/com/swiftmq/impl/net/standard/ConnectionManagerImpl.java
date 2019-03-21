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

package com.swiftmq.impl.net.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.net.*;
import com.swiftmq.swiftlet.threadpool.*;
import com.swiftmq.swiftlet.trace.*;

import java.text.*;
import java.util.*;

public class ConnectionManagerImpl implements ConnectionManager
{
  DecimalFormat formatter = new DecimalFormat("###,##0.00", new DecimalFormatSymbols(Locale.US));

  TraceSwiftlet traceSwiftlet = null;
  TraceSpace traceSpace = null;
  ThreadpoolSwiftlet threadpoolSwiftlet = null;

  EntityList usageList;
  HashSet connections = new HashSet();

  long lastCollectTime = -1;

  protected ConnectionManagerImpl(EntityList usageList)
  {
    this.usageList = usageList;
    usageList.setEntityRemoveListener(new EntityChangeAdapter(null)
    {
      public void onEntityRemove(Entity parent, Entity delEntity)
        throws EntityRemoveException
      {
        Connection myConnection = (Connection) delEntity.getDynamicObject();
        removeConnection(myConnection);
      }
    });

    threadpoolSwiftlet = (ThreadpoolSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$threadpool");

    traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
    traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/created");
  }

  public synchronized int getNumberConnections()
  {
    return connections.size();
  }

  public synchronized void addConnection(Connection connection)
  {
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/addConnection: " + connection);
    Entity ce = usageList.createEntity();
    ce.setName(connection.toString());
    ce.setDynamicObject(connection);
    ce.createCommands();
    try
    {
      Property prop = ce.getProperty("swiftlet");
      prop.setValue(connection.getMetaData().getSwiftlet().getName());
      prop = ce.getProperty("connect-time");
      prop.setValue(new Date().toString());
      usageList.addEntity(ce);
    } catch (Exception ignored)
    {
    }
    connections.add(connection);
  }

  public synchronized void removeConnection(Connection connection)
  {
    // possible during shutdown/reboot
    if (connection == null)
      return;
    if (!(connection.isMarkedForClose() || connection.isClosed()))
    {
      connection.setMarkedForClose();
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/removeConnection: " + connection);
      threadpoolSwiftlet.dispatchTask(new Disconnector(connection));
    }
  }

  public synchronized void removeAllConnections()
  {
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/removeAllConnections");
    Set cloned = (Set)connections.clone();
    for (Iterator iter = cloned.iterator(); iter.hasNext();)
    {
      deleteConnection((Connection) iter.next());
    }
    connections.clear();
    if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/removeAllConnections, done.");
  }

  private void deleteConnection(Connection connection)
  {
    usageList.removeDynamicEntity(connection);
    synchronized (this)
    {
      connections.remove(connection);
    }
    if (!connection.isClosed())
    {
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/deleteConnection: " + connection);
      connection.close();
      if (traceSpace.enabled) traceSpace.trace("sys$net", toString() + "/deleteConnection: " + connection + ", DONE.");
    }
  }

  public void clearLastCollectTime()
  {
    lastCollectTime = -1;
  }

  public synchronized void collectByteCounts()
  {
    long actTime = System.currentTimeMillis();
    double deltas = (actTime - lastCollectTime) / 1000;
    for (Iterator iter = usageList.getEntities().entrySet().iterator(); iter.hasNext();)
    {
      Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
      Property input = entity.getProperty("throughput-input");
      Property output = entity.getProperty("throughput-output");
      Connection connection = (Connection) entity.getDynamicObject();
      if (connection != null)
      {
        Countable in = (Countable) connection.getInputStream();
        Countable out = (Countable) connection.getOutputStream();
        if (lastCollectTime != -1)
        {
          try
          {
            input.setValue(formatter.format(new Double(((double) in.getByteCount() / 1024.0) / deltas)));
            output.setValue(formatter.format(new Double(((double) out.getByteCount() / 1024.0) / deltas)));
          } catch (Exception ignored)
          {
          }
        } else
        {
          try
          {
            input.setValue(new Double(0.0));
            output.setValue(new Double(0.0));
          } catch (Exception ignored)
          {
          }
        }
        in.resetByteCount();
        out.resetByteCount();
      }
    }
    lastCollectTime = actTime;
  }

  public String toString()
  {
    return "ConnectionManager";
  }

  private class Disconnector implements AsyncTask
  {
    Connection connection = null;

    Disconnector(Connection connection)
    {
      this.connection = connection;
    }

    public boolean isValid()
    {
      return !connection.isClosed();
    }

    public String getDispatchToken()
    {
      return NetworkSwiftletImpl.TP_CONNMGR;
    }

    public String getDescription()
    {
      return "sys$net/ConnectionManager/Disconnector for Connection: " + connection;
    }

    public void stop()
    {
    }

    public void run()
    {
      deleteConnection(connection);
    }
  }
}

