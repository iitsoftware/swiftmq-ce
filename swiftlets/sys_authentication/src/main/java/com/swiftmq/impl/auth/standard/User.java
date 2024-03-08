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

import com.swiftmq.swiftlet.auth.ResourceLimitException;
import com.swiftmq.swiftlet.auth.ResourceLimitGroup;
import com.swiftmq.tools.collection.ConcurrentList;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class User {
    String name;
    String password;
    Group group;
    ResourceLimitGroup rlgroup;
    List<String> hostList = new ConcurrentList<>(new ArrayList<>());
    final AtomicInteger numberConnections = new AtomicInteger();

    protected User(String name, String password, Group group, ResourceLimitGroup rlgroup) {
        this.name = name;
        this.password = password;
        this.group = group;
        this.rlgroup = rlgroup;
    }

    public String getName() {
        return (name);
    }

    public String getPassword() {
        return (password);
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Group getGroup() {
        return (group);
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public ResourceLimitGroup getResourceLimitGroup() {
        return (rlgroup);
    }

    public void setResourceLimitGroup(ResourceLimitGroup rlgroup) {
        this.rlgroup = rlgroup;
    }

    public int getNumberConnections() {
        return numberConnections.get();
    }

    public void incNumberConnections() throws ResourceLimitException {
        rlgroup.verifyConnectionLimit(numberConnections.get());
        numberConnections.getAndIncrement();
    }

    public void decNumberConnections() {
        if (numberConnections.get() > 0)
            numberConnections.getAndDecrement();
    }

    public void addHost(String predicate) {
        hostList.add(predicate);
    }

    public void removeHost(String predicate) {
        hostList.remove(predicate);
    }

    public boolean isHostAllowed(String hostname) {
        if (hostList.size() == 0)
            return true;
        for (String s : hostList) {
            if (LikeComparator.compare(hostname, s, '\\'))
                return true;
        }
        return false;
    }

    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("[User, name=");
        s.append(name);
        s.append(", password=");
        s.append(password);
        s.append(", group=");
        s.append(group);
        s.append(", rlgroup=");
        s.append(rlgroup);
        s.append(", numberConnections=");
        s.append(numberConnections.get());
        s.append(", hostList=");
        s.append(hostList);
        s.append("]");
        return s.toString();
    }
}

