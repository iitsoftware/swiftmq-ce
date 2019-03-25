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

package com.swiftmq.swiftlet.routing.event;

import java.util.EventListener;

/**
 * A listener interested in routing events.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public interface RoutingListener extends EventListener {

    /**
     * Called when a destination (remote router) is added.
     *
     * @param evt event.
     */
    public void destinationAdded(RoutingEvent evt);

    /**
     * Called when a destination (remote router) is removed.
     *
     * @param evt event.
     */
    public void destinationRemoved(RoutingEvent evt);

    /**
     * Called when a destination (remote router) is activated.
     *
     * @param evt event.
     */
    public void destinationActivated(RoutingEvent evt);

    /**
     * Called when a destination (remote router) is deactivated.
     *
     * @param evt event.
     */
    public void destinationDeactivated(RoutingEvent evt);
}

