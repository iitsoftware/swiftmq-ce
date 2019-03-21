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

package com.swiftmq.impl.routing.single.route;

import java.util.*;

public class RouteFilter
{
  static final int INCLUDE_BY_HOP = 0;
  static final int EXCLUDE_BY_HOP = 1;
  static final int INCLUDE_BY_DEST = 2;
  static final int EXCLUDE_BY_DEST = 3;

  int type;
  Set routerNames = new HashSet();

  public RouteFilter(int type)
  {
    this.type = type;
  }

  public synchronized void addRouterName(String routerName)
  {
    routerNames.add(routerName);
  }

  public synchronized void removeRouterName(String routerName)
  {
    routerNames.remove(routerName);
  }

  public synchronized boolean isSendable(Route route)
  {
    boolean rc = false;
    switch (type)
    {
      case INCLUDE_BY_HOP:
        rc = routerNames.contains(route.getLastHop());
        break;
      case EXCLUDE_BY_HOP:
        rc = !routerNames.contains(route.getLastHop());
        break;
      case INCLUDE_BY_DEST:
        rc = routerNames.contains(route.getDestinationRouter());
        break;
      case EXCLUDE_BY_DEST:
        rc = !routerNames.contains(route.getDestinationRouter());
        break;
    }
    return rc;
  }

  public String toString()
  {
    String s = null;
    switch (type)
    {
      case INCLUDE_BY_HOP:
        s = "INCLUDE_BY_HOP";
        break;
      case EXCLUDE_BY_HOP:
        s = "EXCLUDE_BY_HOP";
        break;
      case INCLUDE_BY_DEST:
        s = "INCLUDE_BY_DEST";
        break;
      case EXCLUDE_BY_DEST:
        s = "EXCLUDE_BY_DEST";
        break;
    }
    StringBuffer a = new StringBuffer();
    boolean first = true;
    a.append("[");
    synchronized (routerNames)
    {
      for (Iterator iter = routerNames.iterator(); iter.hasNext();)
      {
        if (first)
          first = false;
        else
          a.append(", ");
        a.append(iter.next());
      }
    }
    a.append("]");

    return "[RouteFilter, type=" + s + ", routerNames=" + a.toString() + "]";
  }
}

