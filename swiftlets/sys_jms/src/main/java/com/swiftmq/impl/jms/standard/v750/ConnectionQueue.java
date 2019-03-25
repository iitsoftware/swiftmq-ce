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

package com.swiftmq.impl.jms.standard.v750;

import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;
import com.swiftmq.tools.requestreply.Request;

public class ConnectionQueue extends SingleProcessorQueue {
    ThreadPool pool;
    JMSConnection connection;
    QueueProcessor queueProcessor;

    ConnectionQueue(ThreadPool pool, JMSConnection connection) {
        super(100);
        this.pool = pool;
        this.connection = connection;
        queueProcessor = new QueueProcessor();
    }

    protected void startProcessor() {
        if (!connection.closed)
            pool.dispatchTask(queueProcessor);
    }

    protected void process(Object[] bulk, int n) {
        for (int i = 0; i < n; i++) {
            if (!connection.closed)
                ((Request) bulk[i]).accept(connection.visitor);
            else
                break;
        }
    }

    private class QueueProcessor implements AsyncTask {
        public boolean isValid() {
            return !connection.closed;
        }

        public String getDispatchToken() {
            return Session.TP_SESSIONSVC;
        }

        public String getDescription() {
            return connection.toString() + "/QueueProcessor";
        }

        public void stop() {
        }

        public void run() {
            if (!connection.closed && dequeue())
                pool.dispatchTask(this);
        }
    }
}

