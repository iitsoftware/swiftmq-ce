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

package com.swiftmq.tools.pipeline;


import com.swiftmq.swiftlet.threadpool.AsyncTask;
import com.swiftmq.swiftlet.threadpool.ThreadPool;
import com.swiftmq.tools.queue.SingleProcessorQueue;

public class PipelineQueue extends SingleProcessorQueue {
    ThreadPool myTP = null;
    String dispatchToken = null;
    POVisitor visitor = null;
    boolean closed = false;
    QueueProcessor queueProcessor = null;

    public PipelineQueue(ThreadPool myTP, String dispatchToken, POVisitor visitor) {
        this.myTP = myTP;
        this.dispatchToken = dispatchToken;
        this.visitor = visitor;
        queueProcessor = new QueueProcessor();
        startQueue();
    }

    protected void startProcessor() {
        myTP.dispatchTask(queueProcessor);
    }

    protected void process(Object[] bulk, int n) {
        for (int i = 0; i < n; i++) {
            ((POObject) bulk[i]).accept(visitor);
        }
    }

    public synchronized void close() {
        super.close();
        closed = true;
    }

    private class QueueProcessor implements AsyncTask {

        public boolean isValid() {
            return !closed;
        }

        public String getDispatchToken() {
            return dispatchToken;
        }

        public String getDescription() {
            return "PipelineQueue, dispatchToken=" + dispatchToken;
        }

        public void stop() {
        }

        public void run() {
            if (dequeue() && !closed)
                myTP.dispatchTask(this);
        }
    }
}
