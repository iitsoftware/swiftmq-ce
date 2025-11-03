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
import com.swiftmq.impl.mqtt.pubsub.SubscriptionStoreEntry;
import com.swiftmq.jms.MapMessageImpl;
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TextMessageImpl;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.util.IdGenerator;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SessionStore {
    static final String STORE_QUEUE = "sys$mqtt_sessionstore";
    static final String PROP_CLIENTID = "clientid";
    static final String MSGID = "sys$mqtt/";

    SwiftletContext ctx;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SessionStore(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        if (!ctx.queueManager.isQueueDefined(STORE_QUEUE))
            ctx.queueManager.createQueue(STORE_QUEUE, (ActiveLogin) null);
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", created");
    }

    public Map<String, MQTTSession> load() throws Exception {
        lock.writeLock().lock();
        try {
            Map<String, MQTTSession> result = new HashMap<String, MQTTSession>();
            QueueReceiver receiver = ctx.queueManager.createQueueReceiver(STORE_QUEUE, null, null);
            QueuePullTransaction t = receiver.createTransaction(false);
            MessageEntry entry = null;
            while ((entry = t.getMessage(0)) != null) {
                MessageImpl msg = entry.getMessage();
                String clientId = msg.getStringProperty(PROP_CLIENTID);

                // Check message type - old format uses TextMessage with XStream XML
                if (msg instanceof TextMessageImpl) {
                    // Old XStream format - skip and log warning
                    ctx.logSwiftlet.logWarning(ctx.mqttSwiftlet.getName(),
                            this + ", skipping old XStream-serialized session for clientId=" + clientId +
                            " (TextMessage format no longer supported)");
                    continue;
                }

                // New MapMessage format with JSON
                if (msg instanceof MapMessageImpl) {
                    MapMessageImpl mapMsg = (MapMessageImpl) msg;

                    // Deserialize from MapMessage
                    int durableId = mapMsg.getInt("durableId");
                    int pid = mapMsg.getInt("pid");
                    String subscriptionsJson = mapMsg.getString("subscriptions");

                    List<SubscriptionStoreEntry> subscriptionStoreEntries = new ArrayList<>();
                    JSONArray jsonArray = new JSONArray(subscriptionsJson);
                    for (int i = 0; i < jsonArray.length(); i++) {
                        JSONObject obj = jsonArray.getJSONObject(i);
                        subscriptionStoreEntries.add(new SubscriptionStoreEntry(
                                obj.getString("topicName"),
                                obj.getString("topicNameTranslated"),
                                obj.getInt("qos"),
                                obj.getInt("subscriptionId")
                        ));
                    }

                    SessionStoreEntry sessionStoreEntry = new SessionStoreEntry(durableId, pid, subscriptionStoreEntries);
                    result.put(clientId, new MQTTSession(ctx, clientId, sessionStoreEntry));
                } else {
                    ctx.logSwiftlet.logWarning(ctx.mqttSwiftlet.getName(),
                            this + ", skipping unknown message type for clientId=" + clientId +
                            ", type=" + msg.getClass().getName());
                }
            }
            t.rollback();
            receiver.close();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", load, result.size=" + result.size());

            return result;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void add(String clientid, MQTTSession session) throws Exception {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", add, clientid=" + clientid);
            QueueSender sender = ctx.queueManager.createQueueSender(STORE_QUEUE, null);
            QueuePushTransaction t = sender.createTransaction();
            MapMessageImpl message = new MapMessageImpl();
            message.setJMSDestination(new QueueImpl(STORE_QUEUE));
            message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            message.setJMSPriority(Message.DEFAULT_PRIORITY);
            message.setJMSExpiration(Message.DEFAULT_TIME_TO_LIVE);
            message.setJMSMessageID(MSGID + IdGenerator.getInstance().nextId('/'));
            message.setStringProperty(PROP_CLIENTID, clientid);

            // Serialize to MapMessage with JSON
            SessionStoreEntry sse = session.getSessionStoreEntry();
            message.setInt("durableId", sse.durableId);
            message.setInt("pid", sse.pid);

            JSONArray jsonArray = new JSONArray();
            for (SubscriptionStoreEntry sub : sse.subscriptionStoreEntries) {
                JSONObject obj = new JSONObject();
                obj.put("topicName", sub.topicName);
                obj.put("topicNameTranslated", sub.topicNameTranslated);
                obj.put("qos", sub.qos);
                obj.put("subscriptionId", sub.subscriptionId);
                jsonArray.put(obj);
            }
            message.setString("subscriptions", jsonArray.toString());

            t.putMessage(message);
            t.commit();
            sender.close();
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void remove(String clientid) throws Exception {
        lock.writeLock().lock();
        try {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + ", remove, clientid=" + clientid);
            QueueReceiver receiver = ctx.queueManager.createQueueReceiver(STORE_QUEUE, null, null);
            QueuePullTransaction t = receiver.createTransaction(false);
            MessageEntry entry = null;
            while ((entry = t.getMessage(0)) != null) {
                MessageImpl msg = entry.getMessage();
                String cid = msg.getStringProperty(PROP_CLIENTID);
                if (cid.equals(clientid)) {
                    QueuePullTransaction t2 = receiver.createTransaction(false);
                    t2.moveToTransaction(entry.getMessageIndex(), t);
                    t2.commit();
                    break;
                }

            }
            t.rollback();
            receiver.close();
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override
    public String toString() {
        return "SessionStore";
    }
}
