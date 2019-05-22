package com.swiftmq.extension.amqpbridge.v100;

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
