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

package com.swiftmq.swiftlet.mgmt;

import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;

import java.util.ArrayList;
import java.util.List;

/**
 * The Management Swiftlet provides an interface to the Management subsystem of SwiftMQ.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public abstract class MgmtSwiftlet extends Swiftlet {
    List listeners = new ArrayList();
    boolean active = false;

    /**
     * Add a management listener
     *
     * @param l management listener.
     */
    public synchronized void addMgmtListener(MgmtListener l) {
        listeners.add(l);
        if (active)
            l.adminToolActivated();
    }

    /**
     * Remove a management listener
     *
     * @param l management listener.
     */
    public synchronized void removeMgmtListener(MgmtListener l) {
        listeners.remove(l);
    }

    /**
     * Remove all management listeners
     */
    protected synchronized void removeAllMgmtListeners() {
        listeners.clear();
    }

    public abstract void fireEvent(boolean activated);

    /**
     * Fires a management event to all management listeners.
     *
     * @param activated states whether an admin tool has been activated or not
     */
    protected synchronized void fireMgmtEvent(boolean activated) {
        for (int i = 0; i < listeners.size(); i++) {
            MgmtListener l = (MgmtListener) listeners.get(i);
            if (activated)
                l.adminToolActivated();
            else
                l.adminToolDeactivated();
        }
        active = activated;
    }

    /**
     * Create a CLIExecutor
     *
     * @return CLIExecutor.
     */
    public abstract CLIExecutor createCLIExecutor();
}

