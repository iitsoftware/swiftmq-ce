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

package com.swiftmq.swiftlet.trace;

/**
 * A TraceSpace.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class TraceSpace {

    /**
     * States whether the space is enabled or not. Swiftlets are checking this
     * flag before calling the <code>trace()</code> method to avoid unnecessary
     * calls and String operations.
     */
    public boolean enabled = false;
    String spaceName = null;
    boolean closed = false;

    /**
     * Creates a new trace space
     *
     * @param spaceName space name
     * @param enabled   enabled or not
     */
    public TraceSpace(String spaceName, boolean enabled) {
        // SBgen: Assign variables
        this.spaceName = spaceName;
        this.enabled = enabled;
        // SBgen: End assign
    }

    /**
     * Returns the space name
     *
     * @return space name
     */
    public String getSpaceName() {
        // SBgen: Get variable
        return (spaceName);
    }

    /**
     * Trace a message to the space. The subEntity parameter specifies what kind
     * of entity is tracing. A Extension Swiftlet would use the Swiftlet name here.
     *
     * @param subEntity sub entity
     * @param message   trace message
     */
    public abstract void trace(String subEntity, String message);

    /**
     * Abstract method which is called during <code>close</code> to close any
     * resources associates with this space (i. e. output streams).
     */
    protected abstract void closeSpaceResources();

    /**
     * Closes the space. After that the space is unusable.
     */
    public void close() {
        enabled = false;
        closed = true;
        closeSpaceResources();
    }
}

