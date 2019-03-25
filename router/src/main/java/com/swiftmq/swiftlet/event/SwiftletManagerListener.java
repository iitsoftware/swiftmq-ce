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

import java.util.EventListener;

/**
 * A listener that is interested in events from the SwiftletManager
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 * @see SwiftletManager
 */
public interface SwiftletManagerListener extends EventListener {
    /**
     * Will be called from the SwiftletManager if a Swiftlet should be started. That
     * means: just before starting the Swiftlet.
     *
     * @param evt swiftlet manager event
     */
    public void swiftletStartInitiated(SwiftletManagerEvent evt);

    /**
     * Will be called from the SwiftletManager if a Swiftlet has been started. That
     * means: just after starting the Swiftlet.
     *
     * @param evt swiftlet manager event
     */
    public void swiftletStarted(SwiftletManagerEvent evt);

    /**
     * Will be called from the SwiftletManager if a Swiftlet should be stopped. That
     * means: just before stopping the Swiftlet.
     *
     * @param evt swiftlet manager event
     */
    public void swiftletStopInitiated(SwiftletManagerEvent evt);

    /**
     * Will be called from the SwiftletManager if a Swiftlet is stopped. That
     * means: just after stopping the Swiftlet.
     *
     * @param evt swiftlet manager event
     */
    public void swiftletStopped(SwiftletManagerEvent evt);
}

