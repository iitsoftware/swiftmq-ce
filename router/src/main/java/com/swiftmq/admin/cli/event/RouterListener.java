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

package com.swiftmq.admin.cli.event;

/**
 * A <tt>RouterListener</u> can be registered at CLI to receive
 * router availability events. If a router changes his states
 * (becomes available/unavailable), CLI will fire a router event
 * and will call the <tt>onRouterEvent</tt> method of all registered
 * listeners. Note that the event notification takes place asynchronous.
 *
 * @author IIT GmbH, Bremen/Germany
 * @since 1.2
 */

public interface RouterListener {
    /**
     * Notification method to be invoked from CLI if a router changes
     * his states (becomes available/unavailable).
     *
     * @param routerName router name
     * @param available  true/false
     */
    public void onRouterEvent(String routerName, boolean available);
}

