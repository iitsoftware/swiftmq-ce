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

package com.swiftmq.amqp.v100.client;

import com.swiftmq.amqp.v100.client.po.POAttachDurableConsumer;
import com.swiftmq.amqp.v100.generated.messaging.addressing.TerminusExpiryPolicy;
import com.swiftmq.tools.concurrent.Semaphore;

/**
 * <p>
 * A durable message consumer, created from a session.
 * </p>
 * <p>
 * A durable consumer is backed by a durabe link that will be in place until unsubscribe is called. This durable link
 * exists when the durable consumer is disconnected and will receive messages. Once a durable consumer reconnects,
 * it receives all messages from the durable link.
 * </p>
 * <p>
 * To detach from a durable link call "detach(true)".
 * </p>
 * <p>
 * A durable link is identified by a container id (specified at connection level) and a link name (specified when creating the
 * durable consumer).
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2011, All Rights Reserved
 */
public class DurableConsumer extends Consumer {
    protected DurableConsumer(Session mySession, String source, String name, int linkCredit, int qoS, DeliveryMemory deliveryMemory) {
        super(mySession, source, name, linkCredit, qoS, deliveryMemory);
    }

    /**
     * Unsubscribes the durable consumer and destroys the durable link.
     *
     * @throws AMQPException
     */
    public void unsubscribe() throws AMQPException {
        if (!closed)
            close();
        Semaphore sem = new Semaphore();
        POAttachDurableConsumer po = new POAttachDurableConsumer(sem, name, source, linkCredit, qoS, false, null, TerminusExpiryPolicy.LINK_DETACH, deliveryMemory);
        mySession.getSessionDispatcher().dispatch(po);
        sem.waitHere();
        if (!po.isSuccess())
            throw new AMQPException(po.getException());
        DurableConsumer c = (DurableConsumer) po.getLink();
        c.close();
    }
}
