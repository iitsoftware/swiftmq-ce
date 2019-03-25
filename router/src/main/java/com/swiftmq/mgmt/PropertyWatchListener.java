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
 * A listener on a Property which will be called after the Property value has changed.
 * This listener is for not-owners for the Property, just to become informed about a change.
 * There is no way to veto against the change. Property owners use a PropertyChangeListener.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see PropertyChangeListener
 */
public interface PropertyWatchListener {

    /**
     * Called after the Property value has been changed.
     *
     * @param prop the Property.
     */
    public void propertyValueChanged(Property prop);
}

