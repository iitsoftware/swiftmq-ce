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

import com.swiftmq.tools.dump.*;
import com.swiftmq.impl.routing.single.connection.stage.StageFactory;

public class RouteConverter extends DumpableFactory
{
  public static final int ROUTE_V400 = 0;

  public Dumpable createDumpable(int dumpId)
  {
    Dumpable d = null;
    switch(dumpId)
    {
      case ROUTE_V400:
        d = new com.swiftmq.impl.routing.single.route.v400.RouteImpl();
        break;
    }
    return d;
  }

  public Route convert(Route route, String toVersion) throws Exception
  {
    if (!route.getVersion().equals(StageFactory.PROT_V400)&&!route.getVersion().equals(StageFactory.PROT_V942))
      throw new Exception("Invalid route version: "+route.getVersion());
    if (!toVersion.equals(StageFactory.PROT_V400)&&!toVersion.equals(StageFactory.PROT_V942))
      throw new Exception("Invalid destination version: "+toVersion);
    return route;
  }

  public Route createRoute(String destinationRouter, String version, int type)
  {
    if (version.equals(StageFactory.PROT_V400)||version.equals(StageFactory.PROT_V942))
      return new com.swiftmq.impl.routing.single.route.v400.RouteImpl(StageFactory.PROT_V400,type,destinationRouter);
    return null;
  }
}
