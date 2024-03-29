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

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.versioning.Versionable;

import javax.naming.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JNDIReplication implements TimerListener {
    SwiftletContext sctx = null;
    String name;
    final AtomicBoolean enabled = new AtomicBoolean(false);
    final AtomicLong keepaliveInterval = new AtomicLong();
    String keepaliveName = null;
    Hashtable env = new Hashtable();
    EntityList propList = null;
    String destinationContext = null;
    String namePrefix = null;
    final AtomicBoolean connected = new AtomicBoolean(false);
    Context ctx = null;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public JNDIReplication(SwiftletContext sctx, String name, boolean enabled, long keepaliveInterval,
                           String keepaliveName, String destinationContext, String namePrefix, EntityList propList) {
        this.sctx = sctx;
        this.name = name;
        this.enabled.set(enabled);
        this.keepaliveInterval.set(keepaliveInterval);
        this.keepaliveName = keepaliveName;
        this.propList = propList;
        this.destinationContext = destinationContext;
        this.namePrefix = namePrefix;
        fillEnv();
        setEnabled(enabled);
        setKeepaliveInterval(keepaliveInterval);
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/created");
    }

    private void fillEnv() {
        Map map = propList.getEntities();
        if (map != null && map.size() > 0) {
            for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                Property p = e.getProperty("value");
                env.put(e.getName(), p.getValue());
            }
        }
        propList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String name = newEntity.getName();
                Property p = newEntity.getProperty("value");
                env.put(name, p.getValue());
                if (enabled.get()) {
                    if (connected.get())
                        disconnect();
                    connect();
                }
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", JNDIReplication.this.toString() + "/onEntityAdd (environment-property): property=" + name);
            }
        });
        propList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String name = delEntity.getName();
                env.remove(name);
                if (enabled.get()) {
                    if (connected.get())
                        disconnect();
                    connect();
                }
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", JNDIReplication.this.toString() + "/onEntityRemove (environment-property): property=" + name);
            }
        });
    }

    public boolean isConnected() {
        return connected.get();
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setEnabled(boolean enabled) {
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/setEnabled " + enabled);
        if (enabled) {
            connect();
            sctx.timerSwiftlet.addTimerListener(keepaliveInterval.get(), this);
        } else {
            sctx.timerSwiftlet.removeTimerListener(this);
            disconnect();
        }
        this.enabled.set(enabled);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getReconnectInterval() {
        return keepaliveInterval.get();
    }

    public void setKeepaliveInterval(long keepaliveInterval) {
        if (enabled.get()) {
            if (this.keepaliveInterval.get() > 0)
                sctx.timerSwiftlet.removeTimerListener(this);
            if (keepaliveInterval > 0)
                sctx.timerSwiftlet.addTimerListener(keepaliveInterval, this);
        }
        this.keepaliveInterval.set(keepaliveInterval);
    }

    public void setKeepaliveName(String keepaliveName) {
        this.keepaliveName = keepaliveName;
    }

    public void setDestinationContext(String destinationContext) {
        this.destinationContext = destinationContext;
    }

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public void performTimeAction() {
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/performTimeAction...");
        if (connected.get())
            keepalive();
        if (!connected.get())
            connect();
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/performTimeAction...done");
    }

    private Context createSubcontext(Context parent, String subcontext) throws Exception {
        Name n = parent.getNameParser("").parse(subcontext);
        Context last = parent;
        for (Enumeration _enum = n.getAll(); _enum.hasMoreElements(); ) {
            String s = (String) _enum.nextElement();
            try {
                last.createSubcontext(s);
            } catch (Exception ignored) {
            }
            last = (Context) last.lookup(s);
        }
        return last;
    }

    public void connect() {
        lock.writeLock().lock();
        try {
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/connect...");
            try {
                InitialContext initCtx = new InitialContext(env);
                if (destinationContext == null || destinationContext.length() == 0)
                    ctx = initCtx;
                else {
                    Context subCtx = null;
                    try {
                        subCtx = createSubcontext(initCtx, destinationContext);
                    } catch (Exception e) {
                        throw new Exception("Subcontext '" + destinationContext + "' not found.");
                    }
                    ctx = subCtx;
                }
                connected.set(true);
                sctx.jndiSwiftlet.replicate(this);
            } catch (Exception e) {
                if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/connect, exception=" + e);
                sctx.logSwiftlet.logWarning("sys$jndi", toString() + "/connect, exception=" + e);
                ctx = null;
                connected.set(false);
            }
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/connect...done");

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void keepalive() {
        lock.writeLock().lock();
        try {
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/keepalive...");
            try {
                ctx.lookup(keepaliveName);
            } catch (NameNotFoundException e) {
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", toString() + "/keepalive, NameNotFoundException, ok!");
            } catch (Exception e1) {
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", toString() + "/connect, exception=" + e1);
                sctx.logSwiftlet.logWarning("sys$jndi", toString() + "/connect, exception=" + e1);
                ctx = null;
                connected.set(false);
            }
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/keepalive...done");
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void disconnect() {
        lock.writeLock().lock();
        try {
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/disconnect...");
            if (connected.get() && ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                }
                ctx = null;
                connected.set(false);
            }
            if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/disconnect...done");
        } finally {
            lock.writeLock().unlock();
        }

    }

    private String addPrefix(String name) {
        if (namePrefix == null || namePrefix.length() == 0)
            return name;
        return namePrefix + name;
    }

    public void bind(String name, Object value) {
        lock.readLock().lock();
        try {
            if (sctx.traceSpace.enabled)
                sctx.traceSpace.trace("sys$jndi", toString() + "/bind, name=" + name + ", value=" + value + "...");
            if (!connected.get())
                return;
            try {
                unbind(name);
            } catch (Exception ignored) {
            }
            try {
                if (value instanceof Versionable)
                    value = ((Versionable) value).createCurrentVersionObject();
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", toString() + "/bind, name=" + name + ", actual object=" + value + "...");
                ctx.bind(addPrefix(name), value);
            } catch (Exception e) {
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", toString() + "/bind, name=" + name + ", value=" + value + ", exception=" + e);
                sctx.logSwiftlet.logWarning("sys$jndi", toString() + "/bind, name=" + name + ", value=" + value + ", exception=" + e);
            }
            if (sctx.traceSpace.enabled)
                sctx.traceSpace.trace("sys$jndi", toString() + "/bind, name=" + name + ", value=" + value + "...done");
        } finally {
            lock.readLock().unlock();
        }

    }


    public void unbind(String name) {
        lock.readLock().lock();
        try {
            if (sctx.traceSpace.enabled)
                sctx.traceSpace.trace("sys$jndi", toString() + "/unbind, name=" + name + "...");
            if (!connected.get())
                return;
            try {
                ctx.unbind(addPrefix(name));
            } catch (NamingException e) {
                if (sctx.traceSpace.enabled)
                    sctx.traceSpace.trace("sys$jndi", toString() + "/unbind, name=" + name + ", exception=" + e);
                sctx.logSwiftlet.logWarning("sys$jndi", toString() + "/unbind, name=" + name + ", exception=" + e);
            }
            if (sctx.traceSpace.enabled)
                sctx.traceSpace.trace("sys$jndi", toString() + "/unbind, name=" + name + "...done");
        } finally {
            lock.readLock().unlock();
        }

    }

    public void close() {
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/close...");
        sctx.timerSwiftlet.removeTimerListener(this);
        disconnect();
        if (sctx.traceSpace.enabled) sctx.traceSpace.trace("sys$jndi", toString() + "/close...done");
    }

    public String toString() {
        return "[JNDIReplication, name=" + name + "]";
    }
}
