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

package com.swiftmq.impl.routing.single.jobs;

import com.swiftmq.swiftlet.scheduler.*;
import com.swiftmq.impl.routing.single.SwiftletContext;

public class JobRegistrar
{
  SwiftletContext ctx = null;
  JobGroup jobGroup = null;

  public JobRegistrar(SwiftletContext ctx)
  {
    this.ctx = ctx;
  }

  public void register()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(),toString()+"/register ...");
    jobGroup = ctx.schedulerSwiftlet.getJobGroup("Routing");
    JobFactory jf = new RoutingConnectorJobFactory(ctx);
    jobGroup.addJobFactory(jf.getName(),jf);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(),toString()+"/register done");
  }

  public void unregister()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(),toString()+"/unregister ...");
    jobGroup.removeAll();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.routingSwiftlet.getName(),toString()+"/unregister done");
  }
}
