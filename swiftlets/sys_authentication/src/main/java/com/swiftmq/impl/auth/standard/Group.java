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

package com.swiftmq.impl.auth.standard;

import java.util.HashMap;
import java.util.Map;

public class Group {
    String name;
    Map queueGrants = new HashMap();
    Map topicGrants = new HashMap();

    protected Group(String name) {
        this.name = name;
    }

    public String getName() {
        return (name);
    }

    synchronized void addQueueResourceGrant(QueueResourceGrant queueResourceGrant) {
        queueGrants.put(queueResourceGrant.getResourceName(), queueResourceGrant);
    }

    synchronized QueueResourceGrant getQueueResourceGrant(String queueName) {
        return (QueueResourceGrant) queueGrants.get(queueName);
    }

    synchronized void removeQueueResourceGrant(String queueName) {
        queueGrants.remove(queueName);
    }

    synchronized void addTopicResourceGrant(TopicResourceGrant topicResourceGrant) {
        topicGrants.put(topicResourceGrant.getResourceName(), topicResourceGrant);
    }

    synchronized TopicResourceGrant getTopicResourceGrant(String topicName) {
        return (TopicResourceGrant) topicGrants.get(topicName);
    }

    synchronized void removeTopicResourceGrant(String topicName) {
        topicGrants.remove(topicName);
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[Group, name=");
        s.append(name);
        s.append(", queueGrants=");
        s.append(queueGrants);
        s.append(", topicGrants=");
        s.append(topicGrants);
        s.append("]");
        return s.toString();
    }
}

