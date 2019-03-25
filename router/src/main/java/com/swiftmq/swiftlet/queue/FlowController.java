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

package com.swiftmq.swiftlet.queue;

/**
 * Abstract base class for flow controllers. A flow controller computes a delay,
 * dependent on throughput, queue backlog, and transaction size.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class FlowController {
    protected int receiverCount = 0;
    protected int queueSize = 0;
    protected long lastDelay = 0;
    protected long sentCount = 0;
    protected long sentCountCalls = 0;
    protected long receiveCount = 0;
    protected long receiveCountCalls = 0;
    protected long timestamp = 0;


    /**
     * Returns the FC start queue size
     *
     * @return start queue size
     */
    public int getStartQueueSize() {
        return 0;
    }

    /**
     * Sets the queue size (message count).
     *
     * @param queueSize queue size.
     */
    public synchronized void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }


    /**
     * Sets the receiver count.
     *
     * @param count receiver count.
     */
    public synchronized void setReceiverCount(int count) {
        if (count == 0) {
            sentCount = 0;
            sentCountCalls = 0;
            receiveCount = 0;
            receiveCountCalls = 0;
            timestamp = 0;
        } else if (receiverCount == 0 && count > 0) {
            timestamp = System.currentTimeMillis();
            sentCount = queueSize;
            sentCountCalls = queueSize;
        }
        receiverCount = count;
    }


    /**
     * Sets the recive message count.
     *
     * @param count recive message count.
     */
    public synchronized void setReceiveMessageCount(int count) {
        if (timestamp != 0) {
            receiveCount += count;
            receiveCountCalls++;
        }
    }


    /**
     * Sets the sent message count.
     *
     * @param count sent message count.
     */
    public synchronized void setSentMessageCount(int count) {
        if (timestamp != 0) {
            sentCount += count;
            sentCountCalls++;
        }
    }


    /**
     * Returns the last computed fc delay.
     *
     * @return delay.
     */
    public synchronized long getLastDelay() {
        return lastDelay;
    }


    /**
     * Computes and returns a new delay.
     *
     * @return delay.
     */
    public abstract long getNewDelay();

    public String toString() {
        StringBuffer b = new StringBuffer("[FlowController, ");
        b.append("timestamp=");
        b.append(timestamp);
        b.append(", receiveCount=");
        b.append(receiveCount);
        b.append(", receiveCountCalls=");
        b.append(receiveCountCalls);
        b.append(", sentCount=");
        b.append(sentCount);
        b.append(", sentCountCalls=");
        b.append(sentCountCalls);
        b.append("]");
        return b.toString();
    }
}

