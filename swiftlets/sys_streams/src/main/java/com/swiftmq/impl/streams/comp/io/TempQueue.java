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

package com.swiftmq.impl.streams.comp.io;

import com.swiftmq.impl.streams.StreamContext;
import com.swiftmq.jms.TemporaryQueueImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.mgmt.EntityRemoveException;

import javax.jms.Destination;

public class TempQueue {
    StreamContext ctx;
    String name;
    String queueName;
    String jndiName;
    boolean registered = false;
    Entity usage;

    /**
     * Internal use.
     */
    public TempQueue(StreamContext ctx, String name) throws Exception {
        this.ctx = ctx;
        this.name = name;
        queueName = ctx.ctx.queueManager.createTemporaryQueue();
        try {
            EntityList inputList = (EntityList) ctx.usage.getEntity("tempqueues");
            usage = inputList.createEntity();
            usage.setName(name);
            usage.createCommands();
            inputList.addEntity(usage);
            usage.getProperty("queuename").setValue(queueName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the name under which this temp queue is stored in the Stream
     *
     * @return name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the queue name of the temporary queue at the Queue Manager Swiftlet
     *
     * @return queue name
     */
    public String queueName() {
        return queueName;
    }

    /**
     * Returns the javax.jms.Destination object to set as a replyTo
     *
     * @return Destination
     */
    public Destination destination() {
        return new TemporaryQueueImpl(queueName, null);
    }

    /**
     * Registers a TemporaryQueue under the given name in JNDI so that JMS clients can look it up and can send
     * Messages to it.
     *
     * @param name name under which it should be registered
     * @return this
     */
    public TempQueue registerJNDI(String name) {
        ctx.ctx.jndiSwiftlet.registerJNDIObject(name, new TemporaryQueueImpl(queueName, null));
        registered = true;
        jndiName = name;
        return this;
    }

    /**
     * Registers a TemporaryQueue under TempQueue.name() in JNDI so that JMS clients can look it up and can send
     * Messages to it.
     *
     * @return this
     */
    public TempQueue registerJNDI() {
        return registerJNDI(name);
    }

    /**
     * Deletes the temporary queue and removes it from JNDI, if registered
     *
     * @throws Exception
     */
    public void delete() {
        try {
            if (usage != null)
                ctx.usage.getEntity("tempqueues").removeEntity(usage);
        } catch (EntityRemoveException e) {
        }
        if (registered)
            ctx.ctx.jndiSwiftlet.deregisterJNDIObject(jndiName);
        try {
            ctx.ctx.queueManager.deleteTemporaryQueue(queueName);
        } catch (Exception e) {
        }
        ctx.stream.removeTempQueue(this);
    }
}
