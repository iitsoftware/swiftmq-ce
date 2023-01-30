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

package com.swiftmq.impl.topic.standard;

import com.swiftmq.impl.topic.standard.announce.TopicAnnounceReceiver;
import com.swiftmq.impl.topic.standard.announce.TopicAnnounceSender;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.auth.AuthenticationSwiftlet;
import com.swiftmq.swiftlet.jndi.JNDISwiftlet;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.routing.RoutingSwiftlet;
import com.swiftmq.swiftlet.store.StoreSwiftlet;
import com.swiftmq.swiftlet.threadpool.ThreadpoolSwiftlet;
import com.swiftmq.swiftlet.timer.TimerSwiftlet;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;

public class TopicManagerContext {
    public TopicManagerImpl topicManager = null;
    public TopicAnnounceSender announceSender = null;
    public TopicAnnounceReceiver announceReceiver = null;
    public TraceSwiftlet traceSwiftlet = null;
    public TraceSpace traceSpace = null;
    public LogSwiftlet logSwiftlet = null;
    public TimerSwiftlet timerSwiftlet = null;
    public AuthenticationSwiftlet authSwiftlet = null;
    public JNDISwiftlet jndiSwiftlet = null;
    public StoreSwiftlet storeSwiftlet = null;
    public ThreadpoolSwiftlet threadpoolSwiftlet = null;
    public QueueManager queueManager = null;
    public RoutingSwiftlet routingSwiftlet = null;
    public EntityList activeDurableList = null;
    public EntityList activeSubscriberList = null;
    public EntityList remoteSubscriberList = null;
    public EntityList announceFilterList = null;
}
