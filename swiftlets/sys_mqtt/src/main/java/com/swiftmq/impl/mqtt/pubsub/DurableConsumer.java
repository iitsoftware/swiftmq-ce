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

package com.swiftmq.impl.mqtt.pubsub;

import com.swiftmq.impl.mqtt.SwiftletContext;
import com.swiftmq.impl.mqtt.session.MQTTSession;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.queue.QueueReceiver;

public class DurableConsumer extends Consumer {

    ActiveLogin activeLogin;
    String durableName;

    public DurableConsumer(SwiftletContext ctx, MQTTSession session, String topicName, int subscriptionId) throws Exception {
        super(ctx, session, topicName, subscriptionId);
    }

    @Override
    protected QueueReceiver createReceiver(int subscriptionId) throws Exception {
        activeLogin = session.getMqttConnection().getActiveLogin();
        durableName = String.valueOf(subscriptionId);
        queueName = ctx.topicManager.subscribeDurable(durableName, topic, null, false, session.getMqttConnection().getActiveLogin());
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/createReceiver, queueName=" + queueName + ", durableName=" + durableName);
        return ctx.queueManager.createQueueReceiver(queueName, activeLogin, null);
    }

    @Override
    public void close() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(ctx.mqttSwiftlet.getName(), this + "/close");
        try {
            if (transaction != null && !transaction.isClosed())
                transaction.rollback();
            if (readTransaction != null && !readTransaction.isClosed())
                readTransaction.rollback();
            if (queueReceiver != null)
                queueReceiver.close();
            ctx.topicManager.deleteDurable(durableName, activeLogin);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "DurableConsumer, topicname=" + topicName + ", queuename=" + queueName;
    }
}
