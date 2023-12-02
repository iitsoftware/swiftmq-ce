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

package com.swiftmq.impl.net.netty;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityChangeAdapter;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.mgmt.Property;
import com.swiftmq.swiftlet.net.Connection;
import com.swiftmq.swiftlet.net.ConnectionManager;
import com.swiftmq.swiftlet.threadpool.AsyncTask;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionManagerImpl implements ConnectionManager {
    private static DecimalFormat formatter = new DecimalFormat("###,##0.00", new DecimalFormatSymbols(Locale.US));

    SwiftletContext ctx;
    Set<Connection> connections = ConcurrentHashMap.newKeySet();

    final AtomicLong lastCollectTime = new AtomicLong(-1);

    protected ConnectionManagerImpl(SwiftletContext ctx) {
        this.ctx = ctx;
        ctx.usageList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Connection myConnection = (Connection) delEntity.getDynamicObject();
                removeConnection(myConnection);
            }
        });

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/created");
    }

    public int getNumberConnections() {
        return connections.size();
    }

    public void addConnection(Connection connection) {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/addConnection: " + connection);
        Entity ce = ctx.usageList.createEntity();
        ce.setName(connection.toString());
        ce.setDynamicObject(connection);
        ce.createCommands();
        try {
            Property prop = ce.getProperty("swiftlet");
            prop.setValue(connection.getMetaData().getSwiftlet().getName());
            prop = ce.getProperty("connect-time");
            prop.setValue(new Date().toString());
            ctx.usageList.addEntity(ce);
        } catch (Exception ignored) {
        }
        connections.add(connection);
    }

    public void removeConnection(Connection connection) {
        // possible during shutdown/reboot
        if (connection == null)
            return;
        if (!(connection.isMarkedForClose() || connection.isClosed())) {
            connection.setMarkedForClose();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/removeConnection: " + connection);
            ctx.threadpoolSwiftlet.dispatchTask(new Disconnector(connection));
        }
    }

    public void removeAllConnections() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/removeAllConnections");
        for (Connection c : connections) {
            deleteConnection(c);
        }
        connections.clear();
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/removeAllConnections, done.");
    }

    private void deleteConnection(Connection connection) {
        ctx.usageList.removeDynamicEntity(connection);
            connections.remove(connection);
        if (!connection.isClosed()) {
            if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$net", toString() + "/deleteConnection: " + connection);
            connection.close();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace("sys$net", toString() + "/deleteConnection: " + connection + ", DONE.");
        }
    }

    public void clearLastCollectTime() {
        lastCollectTime.set(-1);
    }

    public void collectByteCounts() {
        long actTime = System.currentTimeMillis();
        long lastTime = lastCollectTime.getAndSet(actTime);
        double deltas = (actTime - lastTime) / 1000.0;
        for (Object o : ctx.usageList.getEntities().entrySet()) {
            Entity entity = (Entity) ((Map.Entry) o).getValue();
            Property input = entity.getProperty("throughput-input");
            Property output = entity.getProperty("throughput-output");
            Connection connection = (Connection) entity.getDynamicObject();
            if (connection != null) {
                Countable in = (Countable) connection.getInputStream();
                Countable out = (Countable) connection.getOutputStream();
                if (lastTime != -1) {
                    try {
                        input.setValue(formatter.format(((double) in.getByteCount() / 1024.0) / deltas));
                        output.setValue(formatter.format(((double) out.getByteCount() / 1024.0) / deltas));
                    } catch (Exception ignored) {
                    }
                } else {
                    try {
                        input.setValue(0.0);
                        output.setValue(0.0);
                    } catch (Exception ignored) {
                    }
                }
                in.resetByteCount();
                out.resetByteCount();
            }
        }
    }

    public String toString() {
        return "ConnectionManager";
    }

    private class Disconnector implements AsyncTask {
        Connection connection = null;

        Disconnector(Connection connection) {
            this.connection = connection;
        }

        public boolean isValid() {
            return !connection.isClosed();
        }

        public String getDispatchToken() {
            return SwiftletContext.TP_CONNMGR;
        }

        public String getDescription() {
            return "sys$net/ConnectionManager/Disconnector for Connection: " + connection;
        }

        public void stop() {
        }

        public void run() {
            deleteConnection(connection);
        }
    }
}

