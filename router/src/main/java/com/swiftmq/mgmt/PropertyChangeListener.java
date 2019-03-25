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
 * A listener on a Property which will be called before the Property value is changed.
 * This listener is intended for the owner of the Property. There is another
 * PropertyWatchListener which is intended for change notifications to not-owners.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface PropertyChangeListener {

    /**
     * Called before the Property value is changed.
     * It is expected that this method throws a PropertyChangeException if any error occurs.
     * If no exception is thrown, the new value will be set for this Property.
     *
     * @param property the Property.
     * @param oldValue the old value.
     * @param newValue the new value.
     * @throws PropertyChangeException on error.
     */
    public void propertyChanged(Property property, Object oldValue, Object newValue)
            throws PropertyChangeException;
}

