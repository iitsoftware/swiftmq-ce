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

package com.swiftmq.impl.mqtt.session;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.connection.MQTTConnection;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.*;

public class SessionRegistry implements TimerListener {
    SwiftletContext ctx;
    Map<String, MQTTSession> sessions = new HashMap<String, MQTTSession>();
    Map<String, AssociateSessionCallback> callbacks = new HashMap<String, AssociateSessionCallback>();
    ActiveLogin dummyLogin = null;
    Property sessionTimeout = null;

    public SessionRegistry(final SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        this.dummyLogin = ctx.authSwiftlet.createActiveLogin("SessionRegistry", "MQTT");
        this.sessions = ctx.sessionStore.load();
        for (Iterator<Map.Entry<String, MQTTSession>> iter = sessions.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, MQTTSession> entry = iter.next();
            createUsage(entry.getKey(), entry.getValue());
        }
        ctx.usageListSessions.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                MQTTSession mySession = (MQTTSession) sessions.get(delEntity.getName());
                if (mySession != null) {
                    if (mySession.isAssociated())
                        throw new EntityRemoveException("Session can't be deleted while it is being associated with a Connection!");
                    sessions.remove(mySession.getClientId());
                    dummyLogin.setClientId(mySession.getClientId());
                    mySession.destroy(dummyLogin, false);
                    try {
                        ctx.sessionStore.remove(mySession.getClientId());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (ctx.traceSpace.enabled)
                        ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), "onEntityRemove (Session): " + mySession);
                }
            }
        });
        sessionTimeout = ctx.config.getProperty("session-timeout");
        ctx.timerSwiftlet.addTimerListener(1000 * 60 * 30, this);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", created");
    }

    @Override
    public synchronized void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction ...");
        List<MQTTSession> toRemove = new ArrayList<MQTTSession>();
        long currentTime = System.currentTimeMillis();
        long timeout = (Long) sessionTimeout.getValue();
        for (Iterator<Map.Entry<String, MQTTSession>> iter = sessions.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, MQTTSession> entry = iter.next();
            MQTTSession mySession = entry.getValue();
            if (!mySession.isAssociated()) {
                long delta = (mySession.getLastUse() + timeout * 1000 * 60 * 60) - currentTime;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction, session=" + mySession.getClientId() + ", delta=" + delta);
                if (delta <= 0)
                    toRemove.add(mySession);
            }
        }
        for (int i = 0; i < toRemove.size(); i++) {
            MQTTSession mySession = toRemove.get(i);
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction, session timeout for session: " + mySession.getClientId());
            ctx.logSwiftlet.logWarning(ctx.mqttSwiftlet.getName(), toString() + ", session timeout for session: " + mySession.getClientId());
            removeSession(toRemove.get(i).getClientId());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction done");
    }

    public synchronized void associateSession(String clientId, AssociateSessionCallback callback) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " ...");
        MQTTConnection connection = callback.getMqttConnection();
        MQTTSession session = sessions.get(clientId);
        if (session == null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " no session found, create a new one");
            session = new MQTTSession(ctx, clientId, true);
            sessions.put(clientId, session);
            createUsage(clientId, session);
            session.associate(connection);
            callback.associated(session);
        } else if (session.getMqttConnection() != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " session found, associated with another connection, initiate close");
            session.setWasPresent(true);
            callbacks.put(clientId, callback);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(session.getMqttConnection().getConnection());
        } else {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " session found, not associated with another connection, invoke callback");
            session.setWasPresent(true);
            session.associate(connection);
            callback.associated(session);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " done");
    }

    private void createUsage(String clientId, final MQTTSession session) {
        try {
            Entity sessionUsage = ctx.usageListSessions.createEntity();
            sessionUsage.setName(clientId);
            session.fillRegistryUsage(sessionUsage);
            sessionUsage.createCommands();
            ctx.usageListSessions.addEntity(sessionUsage);
        } catch (EntityAddException e) {
        }
    }

    public synchronized void disassociateSession(String clientid, MQTTSession session) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", disassociateSession, clientId=" + clientid + " ...");
        session.associate(null);
        AssociateSessionCallback callback = callbacks.remove(clientid);
        if (callback != null) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", disassociateSession, clientId=" + clientid + " callback found, invoke");
            session.associate(callback.getMqttConnection());
            callback.associated(session);
        }
    }

    public synchronized void removeSession(String clientid) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", removeSession, clientId=" + clientid);
        MQTTSession session = sessions.remove(clientid);
        if (session != null) {
            session.destroy();
            try {
                ctx.usageListSessions.removeEntity(ctx.usageListSessions.getEntity(clientid));
            } catch (EntityRemoveException e) {

            }
        }

    }

    public synchronized void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", close");
        sessions.clear();
        callbacks.clear();
        ctx.timerSwiftlet.removeTimerListener(this);
    }

    @Override
    public String toString() {
        return "SessionRegistry";
    }
}
