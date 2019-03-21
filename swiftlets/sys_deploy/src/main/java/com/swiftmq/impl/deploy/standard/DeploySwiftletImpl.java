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

package com.swiftmq.impl.deploy.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.deploy.DeploySpace;

import java.util.*;

public class DeploySwiftletImpl extends com.swiftmq.swiftlet.deploy.DeploySwiftlet
{
  SwiftletContext ctx = null;
  EntityListEventAdapter spaceAdapter = null;
  Map spaceMap = null;

  public synchronized DeploySpace getDeploySpace(String name)
  {
    return (DeploySpace) spaceMap.get(name);
  }

  private void createSpaceAdapter(EntityList spaceList) throws SwiftletException
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createSpaceAdapter ...");
    spaceAdapter = new EntityListEventAdapter(spaceList, true, true)
    {
      public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException
      {
        try
        {
          DeploySpace space = new DeploySpaceImpl(ctx, newEntity);
          synchronized (DeploySwiftletImpl.this)
          {
            spaceMap.put(newEntity.getName(), space);
          }
        } catch (Exception e)
        {
          throw new EntityAddException(e.toString());
        }
      }

      public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException
      {
        DeploySpace space = null;
        synchronized (DeploySwiftletImpl.this)
        {
          space = (DeploySpace) spaceMap.remove(delEntity.getName());
        }
        if (space != null)
          space.close();
      }
    };
    try
    {
      spaceAdapter.init();
    } catch (Exception e)
    {
      if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createSpaceAdapter, exception: " + e);
      throw new SwiftletException(e.toString());
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createSpaceAdapter done");
  }

  protected void startup(Configuration configuration) throws SwiftletException
  {
    ctx = new SwiftletContext(this, configuration);
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
    spaceMap = new HashMap();
    createSpaceAdapter((EntityList) configuration.getEntity("deploy-spaces"));
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done");
  }

  protected void shutdown() throws SwiftletException
  {
    // true when shutdown while standby
    if (ctx == null)
      return;
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
    try
    {
      spaceAdapter.close();
    } catch (Exception e)
    {
    }
    spaceMap.clear();
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done");
    ctx = null;
  }
}
