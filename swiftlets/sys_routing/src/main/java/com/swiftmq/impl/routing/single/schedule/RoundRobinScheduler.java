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

package com.swiftmq.impl.routing.single.schedule;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RoundRobinScheduler extends DefaultScheduler {
    int next = 0;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public RoundRobinScheduler(SwiftletContext ctx, String destinationRouter, String queueName) {
        super(ctx, destinationRouter, queueName);
    }

    protected RoutingConnection getNextConnection() {
        lock.writeLock().lock();
        try {
            if (connections.isEmpty())
                return null;
            if (next > connections.size() - 1)
                next = 0;
            RoutingConnection rc = connections.isEmpty() ? null : connections.get(next).getRoutingConnection();
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(ctx.routingSwiftlet.getName(), this + "/getNextConnection, rc=" + rc);
            next++;
            return rc;
        } finally {
            lock.writeLock().unlock();
        }

    }

    public String toString() {
        return "[RoundRobinScheduler " + super.toString() + ", next=" + next + "]";
    }
}
