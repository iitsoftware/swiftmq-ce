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

/**
 * A listener to be register at Entities and to get informed about additions and
 * removals. Note that this listener is intended for notification only. There is
 * no intervention possible to prevent additions/removals.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface EntityWatchListener {

    /**
     * Called after the entity has been added.
     *
     * @param parent parent.
     * @param entity new entity.
     */
    public void entityAdded(Entity parent, Entity entity);


    /**
     * Called after the entity has been removed.
     *
     * @param parent parent.
     * @param entity removed entity.
     */
    public void entityRemoved(Entity parent, Entity entity);

}
