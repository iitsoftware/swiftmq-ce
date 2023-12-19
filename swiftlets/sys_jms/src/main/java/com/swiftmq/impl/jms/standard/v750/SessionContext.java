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

import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.threadpool.EventLoop;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

import java.util.concurrent.atomic.AtomicInteger;

public class SessionContext {
    public QueueManager queueManager = null;
    public TopicManager topicManager = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public LogSwiftlet logSwiftlet = null;
    public StoreSwiftlet storeSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public String tracePrefix = null;
    public ActiveLogin activeLogin = null;
    public volatile int ackMode = 0;
    public volatile boolean transacted = false;
    public Entity sessionEntity = null;
    public EventLoop sessionLoop = null;
    public EventLoop outboundLoop = null;
    final AtomicInteger msgsReceived = new AtomicInteger();
    final AtomicInteger msgsSent = new AtomicInteger();
    final AtomicInteger totalMsgsReceived = new AtomicInteger();
    final AtomicInteger totalMsgsSent = new AtomicInteger();

    public int getMsgsReceived() {
        return msgsReceived.getAndSet(0);
    }

    public int getMsgsSent() {
        return msgsSent.getAndSet(0);
    }

    public int getTotalMsgsReceived() {
        return totalMsgsReceived.get();
    }

    public int getTotalMsgsSent() {
        return totalMsgsSent.get();
    }

    public void incMsgsSent(int n) {
        if (msgsSent.get() == Integer.MAX_VALUE)
            msgsSent.set(0);
        msgsSent.addAndGet(n);
        if (totalMsgsSent.get() == Integer.MAX_VALUE)
            totalMsgsSent.set(0);
        totalMsgsSent.addAndGet(n);
    }

    public void incMsgsReceived(int n) {
        if (msgsReceived.get() == Integer.MAX_VALUE)
            msgsReceived.set(0);
        msgsReceived.addAndGet(n);
        if (totalMsgsReceived.get() == Integer.MAX_VALUE)
            totalMsgsReceived.set(0);
        totalMsgsReceived.addAndGet(n);
    }
}

