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

package com.swiftmq.impl.routing.single.connection.v942;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;
import com.swiftmq.impl.routing.single.smqpr.v942.SMQRFactory;
import com.swiftmq.impl.routing.single.smqpr.v942.ThrottleRequest;
import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.concurrent.Semaphore;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;

public class ThrottleQueue extends SingleProcessorQueue {
    static final String TP_THROTTLE = "sys$routing.connection.throttlequeue";
    SwiftletContext ctx;
    RoutingConnection connection;
    boolean closed = false;
    ThreadPool myTP = null;
    QueueProcessor queueProcessor = null;
    SingleProcessorQueue outboundQueue = null;
    Semaphore sem = null;

    public ThrottleQueue(SwiftletContext ctx, RoutingConnection connection) {
        this.ctx = ctx;
        this.connection = connection;
        outboundQueue = connection.getOutboundQueue();
        myTP = ctx.threadpoolSwiftlet.getPool(TP_THROTTLE);
        queueProcessor = new QueueProcessor();
        sem = new Semaphore();
        startQueue();
    }

    protected void startProcessor() {
        myTP.dispatchTask(queueProcessor);
    }

    protected void process(Object[] objects, int len) {
        for (int i = 0; i < len; i++) {
            Request r = (Request) objects[i];
            if (r.getDumpId() == SMQRFactory.THROTTLE_REQ) {
                sem.reset();
                sem.waitHere(((ThrottleRequest) r).getDelay());
            } else {
                outboundQueue.enqueue(r);
            }
            if (closed)
                return;
        }
    }

    public synchronized void close() {
        super.close();
        closed = true;
        sem.notifySingleWaiter();
    }

    private class QueueProcessor implements AsyncTask {

        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return TP_THROTTLE;
        }

        public String getDescription() {
            return ctx.routingSwiftlet.getName() + "/" + connection.toString() + "/ThrottleQueue/QueueProcessor";
        }

        public void stop() {
        }

        public void run() {
            if (dequeue() && !closed)
                myTP.dispatchTask(this);
        }
    }
}
