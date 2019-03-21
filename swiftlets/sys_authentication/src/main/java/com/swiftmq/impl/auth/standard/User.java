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

import com.swiftmq.swiftlet.auth.*;
import com.swiftmq.tools.sql.LikeComparator;

import java.util.*;

public class User
{
  String name;
  String password;
  Group group;
  ResourceLimitGroup rlgroup;
  List hostList = new ArrayList();
  int numberConnections = 0;

  protected User(String name, String password, Group group, ResourceLimitGroup rlgroup)
  {
    this.name = name;
    this.password = password;
    this.group = group;
    this.rlgroup = rlgroup;
  }

  public String getName()
  {
    return (name);
  }

  public synchronized String getPassword()
  {
    return (password);
  }

  public synchronized void setPassword(String password)
  {
    this.password = password;
  }

  public synchronized Group getGroup()
  {
    return (group);
  }

  public synchronized void setGroup(Group group)
  {
    this.group = group;
  }

  public synchronized ResourceLimitGroup getResourceLimitGroup()
  {
    return (rlgroup);
  }

  public synchronized void setResourceLimitGroup(ResourceLimitGroup rlgroup)
  {
    this.rlgroup = rlgroup;
  }

  public synchronized int getNumberConnections()
  {
    return numberConnections;
  }

  public synchronized void incNumberConnections() throws ResourceLimitException
  {
    rlgroup.verifyConnectionLimit(numberConnections);
    numberConnections++;
  }

  public synchronized void decNumberConnections()
  {
    if (numberConnections > 0)
      numberConnections--;
  }

  public synchronized void addHost(String predicate)
  {
    hostList.add(predicate);
  }

  public synchronized void removeHost(String predicate)
  {
    hostList.remove(predicate);
  }

  public synchronized boolean isHostAllowed(String hostname)
  {
    if (hostList.size() == 0)
      return true;
    for (int i = 0; i < hostList.size(); i++)
    {
      String predicate = (String) hostList.get(i);
      if (LikeComparator.compare(hostname, predicate, '\\'))
        return true;
    }
    return false;
  }

  public String toString()
  {
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
    s.append(numberConnections);
    s.append(", hostList=");
    s.append(hostList);
    s.append("]");
    return s.toString();
  }
}

