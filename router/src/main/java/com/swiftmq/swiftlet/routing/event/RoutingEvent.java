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

import com.swiftmq.swiftlet.routing.RoutingSwiftlet;

import java.util.EventObject;


/**
 * A routing event.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class RoutingEvent extends EventObject {
    String destination;


    /**
     * Creates a routing event.
     * Use from the RoutingSwiftlet.
     *
     * @param source      routing swiftlet.
     * @param destination router name.
     */
    public RoutingEvent(RoutingSwiftlet source, String destination) {
        super(source);
        this.destination = destination;
    }


    /**
     * Returns the remote router name.
     *
     * @return router name.
     */
    public String getDestination() {
        // SBgen: Get variable
        return (destination);
    }
}

