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

package com.swiftmq.swiftlet.monitor;

import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityList;
import com.swiftmq.swiftlet.SwiftletManager;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class ConnectionCollector implements Collector
{
  SwiftletContext ctx = null;
  EntityList networkUsage = null;

  public ConnectionCollector(SwiftletContext ctx)
  {
    this.ctx = ctx;
    networkUsage = (EntityList) SwiftletManager.getInstance().getConfiguration("sys$net").getEntity("usage");
  }

  public String getDescription()
  {
    return "CONNECTION COLLECTOR";
  }

  public String[] getColumnNames()
  {
    return new String[]{"Connection Name", "Connect Time"};
  }

  public Map collect()
  {
    Map map = new TreeMap();
    Map entities = networkUsage.getEntities();
    if (entities != null)
    {
      for (Iterator iter = entities.entrySet().iterator(); iter.hasNext();)
      {
        Entity entity = (Entity) ((Map.Entry) iter.next()).getValue();
        map.put(entity.getName(), entity.getProperty("connect-time").getValue());
      }
    }
    return map;
  }
}
