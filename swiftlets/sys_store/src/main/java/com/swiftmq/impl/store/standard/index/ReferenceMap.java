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

package com.swiftmq.impl.store.standard.index;

import com.swiftmq.impl.store.standard.StoreContext;
import com.swiftmq.impl.store.standard.cache.Page;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReferenceMap
{
  StoreContext ctx = null;
  Map map = new HashMap();

  public ReferenceMap(StoreContext ctx)
  {
    this.ctx = ctx;
  }

  public synchronized MessagePageReference getReference(Integer pageNo, boolean create)
  {
    MessagePageReference ref = (MessagePageReference) map.get(pageNo);
    if (ref == null && create)
    {
      ref = new MessagePageReference(ctx, pageNo.intValue(), 0);
      map.put(pageNo, ref);
    }
    return ref;
  }

  public synchronized void removeReferencesLessThan(long lessThan)
  {
    for (Iterator iter = map.entrySet().iterator(); iter.hasNext();)
    {
      MessagePageReference ref = (MessagePageReference) ((Map.Entry) iter.next()).getValue();
      if (ref != null && ref.getRefCount() < lessThan)
        iter.remove();
    }
  }

  public synchronized void removeReference(Integer pageNo)
  {
    map.remove(pageNo);
  }

  public synchronized void dump()
  {
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/dump ...");
    for (Iterator iter = map.entrySet().iterator(); iter.hasNext();)
    {
      Map.Entry entry = (Map.Entry) iter.next();
      Integer rootPageNo = (Integer) entry.getKey();
      MessagePageReference ref = (MessagePageReference) entry.getValue();
      try
      {
        Page p = ctx.cacheManager.fetchAndPin(rootPageNo.intValue());
        MessagePage mp = new MessagePage(p);
        if (ctx.traceSpace.enabled)
          ctx.traceSpace.trace("sys$store", toString() + "/dump, rootPageNo=" + rootPageNo + ", ref=" + ref + ", mp=" + mp);
        ctx.cacheManager.unpin(rootPageNo.intValue());
      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/dump done");
  }

  public synchronized void clear()
  {
    map.clear();
  }

  public String toString()
  {
    return "ReferenceMap";
  }
}
