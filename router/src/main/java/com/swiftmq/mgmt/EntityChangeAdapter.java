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
 * An adapter class that joins EntityAddListener and EntityRemoveListener.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class EntityChangeAdapter implements EntityAddListener, EntityRemoveListener {
    protected Object configObject;


    /**
     * Create a new EntityChangeAdapter.
     * A custom config object can be store at creation time for later use.
     *
     * @param configObject config object.
     */
    public EntityChangeAdapter(Object configObject) {
        // SBgen: Assign variable
        this.configObject = configObject;
    }

    public void onEntityAdd(Entity parent, Entity newEntity)
            throws EntityAddException {
    }

    public Configuration onConfigurationAdd(Entity parent, Entity newEntity)
            throws EntityAddException {
        return null;
    }

    public void onEntityRemove(Entity parent, Entity newEntity)
            throws EntityRemoveException {
    }
}

