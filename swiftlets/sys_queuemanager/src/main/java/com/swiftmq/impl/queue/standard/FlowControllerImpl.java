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

package com.swiftmq.impl.queue.standard;

import com.swiftmq.swiftlet.queue.FlowController;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FlowControllerImpl extends FlowController {
    final AtomicLong maxDelay = new AtomicLong(5000);
    final AtomicInteger startQueueSize = new AtomicInteger();
    final AtomicBoolean lastWasNull = new AtomicBoolean(false);
    final AtomicLong prevAmount = new AtomicLong();
    final AtomicBoolean active = new AtomicBoolean(true);

    public FlowControllerImpl(int startQueueSize, long maxDelay) {
        this.startQueueSize.set(startQueueSize);
        this.maxDelay.set(maxDelay);
    }

    public void active(boolean b) {
        active.set(b);
    }

    public int getStartQueueSize() {
        return active.get() ? startQueueSize.get() : -1;
    }

    public long getLastDelay() {
        return active.get() ? super.getLastDelay() : 0;
    }

    public long getNewDelay() {
        if (!active.get())
            return 0;
        if (receiverCount.get() == 0 ||
                queueSize.get() <= startQueueSize.get()) {
            lastDelay.set(0);
            lastWasNull.set(false);
        } else {
            if (receiveCount.get() == 0) {
                if (lastWasNull.get())
                    prevAmount.addAndGet(10);
                else
                    prevAmount.set(10);
                lastDelay.set(Math.min(maxDelay.get(), prevAmount.get()));
                lastWasNull.set(true);
            } else if (timestamp.get() != 0) {
                lastWasNull.set(false);
                long current = System.currentTimeMillis();
                long sc = sentCount.get();
                int qs = queueSize.get() - startQueueSize.get();
                long delta = current - timestamp.get();
                double stp = (double) delta / (double) sc;
                double rtp = (double) delta / (double) receiveCount.get();
                double deltaTp = rtp - stp;
                double btp = (deltaTp + 2) * qs;
                long delay = (long) (deltaTp * (double) (sc / Math.max(sentCountCalls.get(), 1)));
                long newdelay = delay + (long) btp;
                lastDelay.set(Math.min(maxDelay.get(), Math.max(0, newdelay)));
            }
        }
        return lastDelay.get();
    }

    public String toString() {
        StringBuffer b = new StringBuffer("[FlowControllerImpl ");
        b.append(super.toString());
        b.append(" queueSize=");
        b.append(queueSize.get());
        b.append(", maxDelay=");
        b.append(maxDelay.get());
        b.append(", lastDelay=");
        b.append(lastDelay.get());
        b.append(", startQueueSize=");
        b.append(startQueueSize.get());
        b.append(", lastWasNull=");
        b.append(lastWasNull.get());
        b.append(", prevAmount=");
        b.append(prevAmount.get());
        b.append("]");
        return b.toString();
    }
}

