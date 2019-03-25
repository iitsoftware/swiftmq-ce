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

package com.swiftmq.mgmt;

import java.util.Iterator;
import java.util.Map;

public abstract class EntityListEventAdapter extends EntityChangeAdapter {
    EntityList entityList = null;

    public EntityListEventAdapter(EntityList entityList, Object configObject, boolean add, boolean remove) {
        super(configObject);
        this.entityList = entityList;
        if (add)
            entityList.setEntityAddListener(this);
        if (remove)
            entityList.setEntityRemoveListener(this);
    }

    public EntityListEventAdapter(EntityList entityList, boolean add, boolean remove) {
        this(entityList, null, add, remove);
    }

    public void init() throws Exception {
        Map entities = entityList.getEntities();
        if (entities != null) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                onEntityAdd(entityList, e);
            }
        }
    }

    public void close() throws Exception {
        Map entities = entityList.getEntities();
        if (entities != null) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
                onEntityRemove(entityList, e);
            }
        }
    }
}
