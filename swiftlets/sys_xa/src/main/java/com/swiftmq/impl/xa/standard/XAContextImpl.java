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

package com.swiftmq.impl.xa.standard;

import com.swiftmq.jms.XidImpl;
import com.swiftmq.mgmt.Entity;
import com.swiftmq.mgmt.EntityRemoveException;
import com.swiftmq.swiftlet.xa.XAContext;
import com.swiftmq.tools.concurrent.AtomicWrappingCounterInteger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class XAContextImpl implements XAContext {
    static final AtomicWrappingCounterInteger cnt = new AtomicWrappingCounterInteger(1);
    SwiftletContext ctx = null;
    XidImpl xid = null;
    String signature = null;
    final AtomicBoolean recovered = new AtomicBoolean(false);

    public XAContextImpl(SwiftletContext ctx, XidImpl xid) {
        this.ctx = ctx;
        this.xid = xid;
        signature = xid.toString();
    }

    protected static int incCount() {
        return cnt.getAndIncrement();
    }

    private Entity lookupEntity(String signature) {
        Map entities = ctx.preparedUsageList.getEntities();
        for (Object o : entities.entrySet()) {
            Entity xidEntity = (Entity) ((Map.Entry<?, ?>) o).getValue();
            if ((xidEntity.getProperty("xid").getValue()).equals(signature))
                return xidEntity;
        }
        return null;
    }

    protected void removeUsageEntity() {
        try {
            Entity entity = lookupEntity(signature);
            if (entity != null)
                ctx.preparedUsageList.removeEntity(entity);
        } catch (EntityRemoveException e) {
        }
    }

    public void setRecovered(boolean recovered) {
        this.recovered.set(recovered);
    }

    public boolean isRecovered() {
        return recovered.get();
    }

    public XidImpl getXid() {
        return xid;
    }
}
