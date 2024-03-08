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

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.Property;

import javax.jms.DeliveryMode;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SlowSubscriberCondition {
    static final int PM_ALL = 0;
    static final int PM_PERSISTENT = 1;
    static final int PM_NONPERSISTENT = 2;
    static final int ST_ALL = 0;
    static final int ST_LOCAL = 1;
    static final int ST_REMOTE = 2;

    Entity entity = null;
    final AtomicLong maxMessages = new AtomicLong();
    final AtomicInteger pMode = new AtomicInteger();
    final AtomicInteger sMode = new AtomicInteger();
    final AtomicBoolean disconnectNonDurable = new AtomicBoolean(false);
    final AtomicBoolean disconnectDeleteDurable = new AtomicBoolean(false);

    public SlowSubscriberCondition(Entity entity) {
        this.entity = entity;

        Property prop = entity.getProperty("max-messages");
        maxMessages.set((Long) prop.getValue());
        prop.setPropertyChangeListener((property, oldValue, newValue) -> maxMessages.set((Long) newValue));
        prop = entity.getProperty("persistence-mode");
        pMode.set(pmStringToInt((String) prop.getValue()));
        prop.setPropertyChangeListener((property, oldValue, newValue) -> pMode.set(pmStringToInt((String) newValue)));
        prop = entity.getProperty("subscription-type");
        sMode.set(stStringToInt((String) prop.getValue()));
        prop.setPropertyChangeListener((property, oldValue, newValue) -> sMode.set(stStringToInt((String) newValue)));
        prop = entity.getProperty("disconnect-non-durable-subscriber");
        disconnectNonDurable.set((Boolean) prop.getValue());
        prop.setPropertyChangeListener((property, oldValue, newValue) -> disconnectNonDurable.set((Boolean) newValue));
        prop = entity.getProperty("disconnect-delete-durable-subscriber");
        disconnectDeleteDurable.set(((Boolean) prop.getValue()).booleanValue());
        prop.setPropertyChangeListener((property, oldValue, newValue) -> disconnectDeleteDurable.set((Boolean) newValue));
    }

    public boolean isDisconnectNonDurable() {
        return disconnectNonDurable.get();
    }

    public boolean isDisconnectDeleteDurable() {
        return disconnectDeleteDurable.get();
    }

    public boolean isMatch(int deliveryMode, TopicSubscription topicSubscription) throws Exception {
        return ((pMode.get() == PM_ALL ||
                pMode.get() == PM_PERSISTENT && deliveryMode == DeliveryMode.PERSISTENT ||
                pMode.get() == PM_NONPERSISTENT && deliveryMode == DeliveryMode.NON_PERSISTENT) &&
                (sMode.get() == ST_ALL ||
                        sMode.get() == ST_LOCAL && !topicSubscription.isRemote() ||
                        sMode.get() == ST_REMOTE && topicSubscription.isRemote()) &&
                (maxMessages.get() <= topicSubscription.getNumberSubscriberQueueMessages()));
    }

    private int pmStringToInt(String pmString) {
        if (pmString.equals("all"))
            return PM_ALL;
        if (pmString.equals("persistent"))
            return PM_PERSISTENT;
        return PM_NONPERSISTENT;
    }

    private String pmIntToString(int pm) {
        switch (pm) {
            case PM_ALL:
                return "all";
            case PM_PERSISTENT:
                return "persistent";
            case PM_NONPERSISTENT:
                return "non_persistent";
        }
        return null;
    }

    private int stStringToInt(String stString) {
        if (stString.equals("all"))
            return ST_ALL;
        if (stString.equals("local"))
            return ST_LOCAL;
        return ST_REMOTE;
    }

    private String stIntToString(int st) {
        switch (st) {
            case ST_ALL:
                return "all";
            case ST_LOCAL:
                return "local";
            case ST_REMOTE:
                return "remote";
        }
        return null;
    }

    String getTopicName() {
        return entity.getName();
    }

    public String toString() {
        String b = "[SlowSubscriberCondition, " + "maxMessage=" +
                maxMessages.get() +
                ", pMode=" +
                pmIntToString(pMode.get()) +
                ", sMode=" +
                stIntToString(sMode.get()) +
                "]";
        return b;
    }
}
