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

package com.swiftmq.impl.amqpbridge.v100;

import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;

public class Poller extends SingleProcessorQueue {
    ThreadPool pool = null;
    boolean closed = false;

    public Poller(ThreadPool pool) {
        super(100);
        this.pool = pool;
    }

    protected void startProcessor() {
        if (!closed)
            pool.dispatchTask(new PollTask());
    }

    protected void process(Object[] bulk, int n) {
        for (int i = 0; i < n; i++) {
            BridgeController bridgeController = (BridgeController) bulk[i];
            bridgeController.poll();
        }
    }

    private class PollTask implements AsyncTask {
        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return "PollTask";
        }

        public String getDescription() {
            return "Poller/PollTask";
        }

        public void run() {
            if (!closed && dequeue())
                pool.dispatchTask(this);
        }

        public void stop() {
        }
    }
}
