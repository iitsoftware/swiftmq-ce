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
import com.swiftmq.mgmt.PropertyChangeException;
import com.swiftmq.mgmt.PropertyChangeListener;

import javax.jms.DeliveryMode;

public class SlowSubscriberCondition {
    static final int PM_ALL = 0;
    static final int PM_PERSISTENT = 1;
    static final int PM_NONPERSISTENT = 2;
    static final int ST_ALL = 0;
    static final int ST_LOCAL = 1;
    static final int ST_REMOTE = 2;

    Entity entity = null;
    volatile long maxMessages = 0;
    volatile int pMode = 0;
    volatile int sMode = 0;
    volatile boolean disconnectNonDurable = false;
    volatile boolean disconnectDeleteDurable = false;

    public SlowSubscriberCondition(Entity entity) {
        this.entity = entity;

        Property prop = entity.getProperty("max-messages");
        maxMessages = ((Long) prop.getValue()).longValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                maxMessages = ((Long) newValue).longValue();
            }
        });
        prop = entity.getProperty("persistence-mode");
        pMode = pmStringToInt((String) prop.getValue());
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                pMode = pmStringToInt((String) newValue);
            }
        });
        prop = entity.getProperty("subscription-type");
        sMode = stStringToInt((String) prop.getValue());
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                sMode = stStringToInt((String) newValue);
            }
        });
        prop = entity.getProperty("disconnect-non-durable-subscriber");
        disconnectNonDurable = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                disconnectNonDurable = ((Boolean) newValue).booleanValue();
            }
        });
        prop = entity.getProperty("disconnect-delete-durable-subscriber");
        disconnectDeleteDurable = ((Boolean) prop.getValue()).booleanValue();
        prop.setPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                disconnectDeleteDurable = ((Boolean) newValue).booleanValue();
            }
        });
    }

    public boolean isDisconnectNonDurable() {
        return disconnectNonDurable;
    }

    public boolean isDisconnectDeleteDurable() {
        return disconnectDeleteDurable;
    }

    public boolean isMatch(int deliveryMode, TopicSubscription topicSubscription) throws Exception {
        return ((pMode == PM_ALL ||
                pMode == PM_PERSISTENT && deliveryMode == DeliveryMode.PERSISTENT ||
                pMode == PM_NONPERSISTENT && deliveryMode == DeliveryMode.NON_PERSISTENT) &&
                (sMode == ST_ALL ||
                        sMode == ST_LOCAL && !topicSubscription.isRemote() ||
                        sMode == ST_REMOTE && topicSubscription.isRemote()) &&
                (maxMessages <= topicSubscription.getNumberSubscriberQueueMessages()));
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
        StringBuffer b = new StringBuffer("[SlowSubscriberCondition, ");
        b.append("maxMessage=");
        b.append(maxMessages);
        b.append(", pMode=");
        b.append(pmIntToString(pMode));
        b.append(", sMode=");
        b.append(stIntToString(sMode));
        b.append("]");
        return b.toString();
    }
}
