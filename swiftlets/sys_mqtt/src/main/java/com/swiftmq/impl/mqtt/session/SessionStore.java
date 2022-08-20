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
import com.swiftmq.jms.MessageImpl;
import com.swiftmq.jms.QueueImpl;
import com.swiftmq.jms.TextMessageImpl;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.*;
import com.swiftmq.tools.util.IdGenerator;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Dom4JDriver;
import com.thoughtworks.xstream.security.AnyTypePermission;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import java.util.HashMap;
import java.util.Map;

public class SessionStore {
    static final String STORE_QUEUE = "sys$mqtt_sessionstore";
    static final String PROP_CLIENTID = "clientid";
    static final String MSGID = "sys$mqtt/";

    SwiftletContext ctx;
    XStream xStream;

    public SessionStore(SwiftletContext ctx) throws Exception {
        this.ctx = ctx;
        if (!ctx.queueManager.isQueueDefined(STORE_QUEUE))
            ctx.queueManager.createQueue(STORE_QUEUE, (ActiveLogin) null);
        xStream = new XStream(new Dom4JDriver());
        xStream.addPermission(AnyTypePermission.ANY);
        xStream.allowTypesByWildcard(new String[]{"com.swiftmq.**"});
        xStream.setClassLoader(getClass().getClassLoader());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", created");
    }

    public synchronized Map<String, MQTTSession> load() throws Exception {
        Map<String, MQTTSession> result = new HashMap<String, MQTTSession>();
        QueueReceiver receiver = ctx.queueManager.createQueueReceiver(STORE_QUEUE, null, null);
        QueuePullTransaction t = receiver.createTransaction(false);
        MessageEntry entry = null;
        while ((entry = t.getMessage(0)) != null) {
            TextMessageImpl msg = (TextMessageImpl) entry.getMessage();
            String clientId = msg.getStringProperty(PROP_CLIENTID);
            result.put(clientId, new MQTTSession(ctx, clientId, (SessionStoreEntry) xStream.fromXML(msg.getText())));
        }
        t.rollback();
        receiver.close();
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", load, result.size=" + result.size());

        return result;
    }

    public synchronized void add(String clientid, MQTTSession session) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", add, clientid=" + clientid);
        QueueSender sender = ctx.queueManager.createQueueSender(STORE_QUEUE, null);
        QueuePushTransaction t = sender.createTransaction();
        TextMessageImpl message = new TextMessageImpl();
        message.setJMSDestination(new QueueImpl(STORE_QUEUE));
        message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        message.setJMSPriority(Message.DEFAULT_PRIORITY);
        message.setJMSExpiration(Message.DEFAULT_TIME_TO_LIVE);
        message.setJMSMessageID(MSGID + IdGenerator.getInstance().nextId('/'));
        message.setStringProperty(PROP_CLIENTID, clientid);
        message.setText(xStream.toXML(session.getSessionStoreEntry()));
        t.putMessage(message);
        t.commit();
        sender.close();
    }

    public synchronized void remove(String clientid) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), toString() + ", remove, clientid=" + clientid);
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
    }

    @Override
    public String toString() {
        return "SessionStore";
    }
}
