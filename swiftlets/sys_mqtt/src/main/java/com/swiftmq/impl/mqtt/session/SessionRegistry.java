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
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.timer.event.TimerListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionRegistry implements TimerListener {
    SwiftletContext ctx;
    Map<String, MQTTSession> sessions;
    Map<String, AssociateSessionCallback> callbacks = new ConcurrentHashMap<>();
    ActiveLogin dummyLogin = null;
    Property sessionTimeout = null;

    public SessionRegistry(final SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        this.dummyLogin = ctx.authSwiftlet.createActiveLogin("SessionRegistry", "MQTT");
        this.sessions = ctx.sessionStore.load();
        for (Map.Entry<String, MQTTSession> entry : sessions.entrySet()) {
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
    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction ...");
        List<MQTTSession> toRemove = new ArrayList<MQTTSession>();
        long currentTime = System.currentTimeMillis();
        long timeout = (Long) sessionTimeout.getValue();
        for (Map.Entry<String, MQTTSession> entry : sessions.entrySet()) {
            MQTTSession mySession = entry.getValue();
            if (!mySession.isAssociated()) {
                long delta = (mySession.getLastUse() + timeout * 1000 * 60 * 60) - currentTime;
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction, session=" + mySession.getClientId() + ", delta=" + delta);
                if (delta <= 0)
                    toRemove.add(mySession);
            }
        }
        for (MQTTSession mySession : toRemove) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction, session timeout for session: " + mySession.getClientId());
            ctx.logSwiftlet.logWarning(ctx.mqttSwiftlet.getName(), toString() + ", session timeout for session: " + mySession.getClientId());
            removeSession(mySession.getClientId());
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", performTimeAction done");
    }

    public void associateSession(String clientId, AssociateSessionCallback callback) {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " ...");

        sessions.compute(clientId, (key, session) -> {
            if (session == null) {
                // Session does not exist, create a new one
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " no session found, create a new one");

                MQTTSession newSession = new MQTTSession(ctx, clientId, true);
                createUsage(clientId, newSession);
                newSession.associate(callback.getMqttConnection());
                callback.associated(newSession);
                return newSession;
            } else {
                // Session exists
                handleExistingSession(session, clientId, callback);
                return session;
            }
        });

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " done");
    }

    private void handleExistingSession(MQTTSession session, String clientId, AssociateSessionCallback callback) {
        if (session.getMqttConnection() != null) {
            // Session is associated with another connection
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " session found, associated with another connection, initiate close");

            session.setWasPresent(true);
            callbacks.put(clientId, callback);
            ctx.networkSwiftlet.getConnectionManager().removeConnection(session.getMqttConnection().getConnection());
        } else {
            // Session is not associated with another connection
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", associateSession, clientId=" + clientId + " session found, not associated with another connection, invoke callback");

            session.setWasPresent(true);
            session.associate(callback.getMqttConnection());
            callback.associated(session);
        }
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

    public void disassociateSession(String clientId, MQTTSession session) {
        if (ctx.traceSpace.enabled) {
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", disassociateSession, clientId=" + clientId + " ...");
        }

        session.associate(null); // Ensure this is thread-safe

        AssociateSessionCallback callback = callbacks.remove(clientId);
        if (callback != null) {
            if (ctx.traceSpace.enabled) {
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", disassociateSession, clientId=" + clientId + " callback found, invoke");
            }
            session.associate(callback.getMqttConnection());
            callback.associated(session);
        }
    }

    public void removeSession(String clientid) {
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

    public void close() {
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
