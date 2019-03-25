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

package com.swiftmq.swiftlet.event;

import com.swiftmq.swiftlet.SwiftletManager;

import java.util.EventObject;

/**
 * An event fired by the SwiftletManager
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see SwiftletManager
 */
public class SwiftletManagerEvent extends EventObject {
    String swiftletName;

    /**
     * Constructs a new SwiftletManagerEvent
     *
     * @param swiftletName    The Swiftlet name
     * @param swiftletManager The Swiftlet manager
     * @SBGen Constructor
     */
    public SwiftletManagerEvent(SwiftletManager swiftletManager, String swiftletName) {
        super(swiftletManager);
        this.swiftletName = swiftletName;
    }

    /**
     * Returns the Swiftlet name.
     *
     * @return Swiftlet name
     */
    public String getSwiftletName() {
        return (swiftletName);
    }
}

