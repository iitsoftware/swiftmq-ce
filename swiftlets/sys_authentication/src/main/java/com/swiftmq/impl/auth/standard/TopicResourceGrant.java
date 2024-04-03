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

import java.util.concurrent.atomic.AtomicBoolean;

public class TopicResourceGrant extends ResourceGrant {
    final AtomicBoolean subscriberGranted = new AtomicBoolean(false);
    final AtomicBoolean publisherGranted = new AtomicBoolean(false);
    final AtomicBoolean createDurableGranted = new AtomicBoolean(false);

    public TopicResourceGrant(String resourceName, boolean subscriberGranted, boolean publisherGranted, boolean createDurableGranted) {
        super(resourceName);
        this.subscriberGranted.set(subscriberGranted);
        this.publisherGranted.set(publisherGranted);
        this.createDurableGranted.set(createDurableGranted);
    }

    public void setSubscriberGranted(boolean subscriberGranted) {
        this.subscriberGranted.set(subscriberGranted);
    }

    public boolean isSubscriberGranted() {
        return (subscriberGranted.get());
    }

    public void setPublisherGranted(boolean publisherGranted) {
        this.publisherGranted.set(publisherGranted);
    }

    public boolean isPublisherGranted() {
        return (publisherGranted.get());
    }

    public void setCreateDurableGranted(boolean createDurableGranted) {
        this.createDurableGranted.set(createDurableGranted);
    }

    public boolean isCreateDurableGranted() {
        return (createDurableGranted.get());
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[TopicResourceGrant, resourceName=");
        s.append(resourceName);
        s.append(", subscriberGranted=");
        s.append(subscriberGranted.get());
        s.append(", publisherGranted=");
        s.append(publisherGranted.get());
        s.append(", createDurableGranted=");
        s.append(createDurableGranted.get());
        s.append("]");
        return s.toString();
    }
}

