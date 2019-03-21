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

package com.swiftmq.impl.amqp.amqp.v01_00_00.transformer;

import com.swiftmq.impl.amqp.SwiftletContext;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityAddException;
import com.swiftmq.mgmt.EntityList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TransformerFactory
{
  SwiftletContext ctx = null;
  Entity transformerEntity = null;
  EntityList defaultInboundTransformerList = null;
  EntityList defaultOutboundTransformerList = null;
  EntityList destinationTransformerList = null;

  public TransformerFactory(SwiftletContext ctx)
  {
    this.ctx = ctx;
    transformerEntity = ctx.root.getEntity("declarations").getEntity("transformer");
    defaultInboundTransformerList = (EntityList) transformerEntity.getEntity("default-inbound-transformers");
    createDefaultTransformer("0", defaultInboundTransformerList);
    defaultOutboundTransformerList = (EntityList) transformerEntity.getEntity("default-outbound-transformers");
    createDefaultTransformer("0", defaultOutboundTransformerList);
    destinationTransformerList = (EntityList) transformerEntity.getEntity("destination-transformers");
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", created");
  }

  private void createDefaultTransformer(String key, EntityList list)
  {
    Map entities = list.getEntities();
    if (entities == null || entities.size() == 0)
    {
      Entity newEntity = list.createEntity();
      newEntity.setName(key);
      newEntity.createCommands();
      try
      {
        list.addEntity(newEntity);
      } catch (EntityAddException e)
      {
        e.printStackTrace();
      }
    }
  }

  private Map toMap(EntityList list)
  {
    Map result = new HashMap();
    Map entities = list.getEntities();
    if (entities != null && entities.size() > 0)
    {
      for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); )
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        result.put(e.getName(), e.getProperty("value").getValue());
      }
    }
    return result;
  }

  public InboundTransformer createInboundTransformer(long messageFormat, String destination) throws Exception
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createInboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + " ...");
    Entity result = null;
    Entity dest = destinationTransformerList.getEntity(destination);
    if (dest != null)
    {
      EntityList inboundList = (EntityList) dest.getEntity("inbound-transformers");
      if (inboundList != null)
        result = inboundList.getEntity(String.valueOf(messageFormat));
    }
    if (result == null)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createInboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + ", no destination transformer found, using default");
      result = defaultInboundTransformerList.getEntity(String.valueOf(messageFormat));
    }
    InboundTransformer inboundTransformer = null;
    if (result != null)
    {
      inboundTransformer = (InboundTransformer) Class.forName((String) result.getProperty("class-name").getValue()).newInstance();
      inboundTransformer.setConfiguration(toMap((EntityList) result.getEntity("properties")));
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createInboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + " returns " + inboundTransformer);
    return inboundTransformer;

  }

  public OutboundTransformer createOutboundTransformer(long messageFormat, String destination) throws Exception
  {
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createOutboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + " ...");
    Entity result = null;
    Entity dest = destinationTransformerList.getEntity(destination);
    if (dest != null)
    {
      EntityList outboundList = (EntityList) dest.getEntity("outbound-transformers");
      if (outboundList != null)
        result = outboundList.getEntity(String.valueOf(messageFormat));
    }
    if (result == null)
    {
      if (ctx.traceSpace.enabled)
        ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createOutboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + ", no destination transformer found, using default");
      result = defaultOutboundTransformerList.getEntity(String.valueOf(messageFormat));
    }
    OutboundTransformer outboundTransformer = null;
    if (result != null)
    {
      outboundTransformer = (OutboundTransformer) Class.forName((String) result.getProperty("class-name").getValue()).newInstance();
      outboundTransformer.setConfiguration(toMap((EntityList) result.getEntity("properties")));
    }
    if (ctx.traceSpace.enabled)
      ctx.traceSpace.trace(ctx.amqpSwiftlet.getName(), toString() + ", createOutboundTransformer, messageFormat=" + messageFormat + ", destination=" + destination + " returns " + outboundTransformer);
    return outboundTransformer;
  }

  public String toString()
  {
    return "TransformerFactory";
  }
}
