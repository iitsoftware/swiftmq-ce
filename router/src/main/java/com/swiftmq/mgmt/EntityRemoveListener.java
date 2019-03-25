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
 * A listener on an Entity which will be called before the Entity is removed.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface EntityRemoveListener {

    /**
     * Called before the Entity is removed.
     * The listener should do the appropriate work and should throw an EntityRemoveException
     * if there is any reason that prevents the removal. If no exception is thrown, the
     * entity will be removed from the parent.
     *
     * @param parent    the parent.
     * @param delEntity the entity to be delete.
     * @throws EntityRemoveException if there is any reason that prevents the removal.
     */
    public void onEntityRemove(Entity parent, Entity delEntity)
            throws EntityRemoveException;
}

