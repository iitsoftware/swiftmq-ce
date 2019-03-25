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

package com.swiftmq.impl.jms.standard.v500;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.swiftlet.auth.ActiveLogin;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.topic.TopicManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.tools.queue.SingleProcessorQueue;

public class SessionContext {
    public QueueManager queueManager = null;
    public TopicManager topicManager = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public LogSwiftlet logSwiftlet = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public String tracePrefix = null;
    public ActiveLogin activeLogin = null;
    public int ackMode = 0;
    public boolean transacted = false;
    public Entity sessionEntity = null;
    public SingleProcessorQueue sessionQueue = null;
    public SingleProcessorQueue connectionOutboundQueue = null;
    public volatile int msgsReceived = 0;
    public volatile int msgsSent = 0;

    public int getMsgsReceived() {
        int n = msgsReceived;
        msgsReceived = 0;
        return n;
    }

    public int getMsgsSent() {
        int n = msgsSent;
        msgsSent = 0;
        return n;
    }

    public void incMsgsSent(int n) {
        if (msgsSent == Integer.MAX_VALUE)
            msgsSent = 0;
        msgsSent += n;
    }

    public void incMsgsReceived(int n) {
        if (msgsReceived == Integer.MAX_VALUE)
            msgsReceived = 0;
        msgsReceived += n;
    }
}

