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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReferenceMap {
    StoreContext ctx = null;
    Map<Integer, MessagePageReference> map = new ConcurrentHashMap<>();

    public ReferenceMap(StoreContext ctx) {
        this.ctx = ctx;
    }

    public MessagePageReference getReference(Integer pageNo, boolean create) {
        return create ? map.computeIfAbsent(pageNo, key -> new MessagePageReference(ctx, key, 0)) : map.get(pageNo);
    }

    public void removeReferencesLessThan(long lessThan) {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
            MessagePageReference ref = (MessagePageReference) ((Map.Entry) iter.next()).getValue();
            if (ref != null && ref.getRefCount() < lessThan)
                iter.remove();
        }
    }

    public void removeReference(Integer pageNo) {
        map.remove(pageNo);
    }

    public void dump() {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/dump ...");
        for (Map.Entry<Integer, MessagePageReference> integerMessagePageReferenceEntry : map.entrySet()) {
            Map.Entry<Integer, MessagePageReference> entry = integerMessagePageReferenceEntry;
            Integer rootPageNo = entry.getKey();
            MessagePageReference ref = entry.getValue();
            try {
                Page p = ctx.cacheManager.fetchAndPin(rootPageNo);
                MessagePage mp = new MessagePage(p);
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace("sys$store", toString() + "/dump, rootPageNo=" + rootPageNo + ", ref=" + ref + ", mp=" + mp);
                ctx.cacheManager.unpin(rootPageNo);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace("sys$store", toString() + "/dump done");
    }

    public void clear() {
        map.clear();
    }

    public String toString() {
        return "ReferenceMap";
    }
}
