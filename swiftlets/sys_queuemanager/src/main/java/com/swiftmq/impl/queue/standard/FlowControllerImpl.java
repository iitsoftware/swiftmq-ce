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

import com.swiftmq.swiftlet.queue.*;

public class FlowControllerImpl extends FlowController
{
  long maxDelay = 5000;
  int startQueueSize = 0;
  boolean lastWasNull = false;
  long prevAmount = 0;

  public FlowControllerImpl(int startQueueSize, long maxDelay)
  {
    this.startQueueSize = startQueueSize;
    this.maxDelay = maxDelay;
  }

  public synchronized int getStartQueueSize()
  {
    return startQueueSize;
  }

  public synchronized long getLastDelay()
  {
    return super.getLastDelay();
  }

  public synchronized long getNewDelay()
  {
    if (receiverCount == 0 ||
      queueSize <= startQueueSize)
    {
      lastDelay = 0;
      lastWasNull = false;
    } else
    {
      if (receiveCount == 0)
      {
        if (lastWasNull)
          prevAmount += 10;
        else
          prevAmount = 10;
        lastDelay = Math.min(maxDelay, prevAmount);
        lastWasNull = true;
      } else if (timestamp != 0)
      {
        lastWasNull = false;
        long current = System.currentTimeMillis();
        long sc = sentCount;
        int qs = queueSize - startQueueSize;
        long delta = current - timestamp;
        double stp = (double) delta / (double) sc;
        double rtp = (double) delta / (double) receiveCount;
        double deltaTp = rtp - stp;
        double btp = (deltaTp + 2) * qs;
        long delay = (long) (deltaTp * (double) (sc / Math.max(sentCountCalls, 1)));
        long newdelay = delay + (long) btp;
        lastDelay = Math.min(maxDelay, Math.max(0, newdelay));
      }
    }
    return lastDelay;
  }

  public String toString()
  {
    StringBuffer b = new StringBuffer("[FlowControllerImpl ");
    b.append(super.toString());
    b.append(" queueSize=");
    b.append(queueSize);
    b.append(", maxDelay=");
    b.append(maxDelay);
    b.append(", lastDelay=");
    b.append(lastDelay);
    b.append(", startQueueSize=");
    b.append(startQueueSize);
    b.append(", lastWasNull=");
    b.append(lastWasNull);
    b.append(", prevAmount=");
    b.append(prevAmount);
    b.append("]");
    return b.toString();
  }
}

