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

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;

import java.util.HashMap;


/**
 * The TraceSwiftlet manages trace spaces. A trace space is a destination for
 * trace output and can be dynamically switched on/off. Therefore, every Swiftlet
 * should place trace call into their code at relevant places to enable live tracing
 * if necessary.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class TraceSwiftlet extends Swiftlet {
    public final static String SPACE_KERNEL = "kernel";
    public final static String SPACE_SWIFTLET = "swiftlet";
    public final static String SPACE_QUEUE = "queue";
    public final static String SPACE_PROTOCOL = "protocol";
    /**
     * @SBGen Collection of com.swiftmq.swiftlet.trace.TraceSpace
     */
    HashMap traceSpaces = new HashMap();
    Object semaphore = new Object();

    /**
     * Get a trace space with that name.
     *
     * @param spaceName space name
     * @return always returns a valid trace space
     */
    public TraceSpace getTraceSpace(String spaceName) {
        TraceSpace space = null;
        synchronized (semaphore) {
            space = (TraceSpace) traceSpaces.get(spaceName);
            if (space == null) {
                space = createTraceSpace(spaceName);
                traceSpaces.put(spaceName, space);
            }
        }
        return space;
    }

    /**
     * Enabled/Disables the trace space. If the space does not exists a new one
     * is created.
     *
     * @param spaceName space name
     * @param b         true or false
     */
    public void setTraceEnabled(String spaceName, boolean b) {
        TraceSpace space = getTraceSpace(spaceName);
        space.enabled = b;
    }

    /**
     * Abstract factory method to create a trace space. In every case it has to
     * return a valid trace space object
     *
     * @param spaceName space name
     * @return a valid trace space object
     */
    protected abstract TraceSpace createTraceSpace(String spaceName);

    protected void shutdown()
            throws SwiftletException {
        traceSpaces.clear();
    }
}

