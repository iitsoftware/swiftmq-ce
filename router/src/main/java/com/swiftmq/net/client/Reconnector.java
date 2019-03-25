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

package com.swiftmq.net.client;

import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class Reconnector {
    List servers = null;
    Map parameters = null;
    boolean enabled = false;
    int maxRetries = 0;
    long retryDelay = 0;
    boolean debug = false;
    Connection active = null;
    int currentPos = 0;
    boolean closed = false;
    boolean firstConnectAttempt = true;
    String debugString = null;

    protected Reconnector(List servers, Map parameters, boolean enabled, int maxRetries, long retryDelay, boolean debug) {
        this.servers = servers;
        this.parameters = parameters;
        this.enabled = enabled;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        this.debug = debug;
        if (debug)
            System.out.println(dbg() + " created, enabled=" + enabled + ", maxRetries=" + maxRetries + ", retryDelay=" + retryDelay + ", servers=" + servers + ", parameters=" + parameters);
    }

    private String dbg() {
        return new Date() + " " + (debugString == null ? toString() : debugString);
    }

    public void setDebugString(String debugString) {
        this.debugString = "[Reconnector, " + debugString + "]";
    }

    public List getServers() {
        return servers;
    }

    public boolean isDebug() {
        return debug;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isIntraVM() {
        return false;
    }

    protected abstract Connection createConnection(ServerEntry entry, Map parameters);

    public synchronized Connection getConnection() {
        if (debug) System.out.println(dbg() + ", getConnection ...");
        int nRetries = -1;
        while (!closed && active == null && nRetries < maxRetries) {
            if (retryDelay > 0 && !firstConnectAttempt) {
                try {
                    if (debug)
                        System.out.println(dbg() + ", nRetries=" + nRetries + ", waiting " + retryDelay + " ms ...");
                    wait(retryDelay);
                } catch (InterruptedException e) {
                }
            }
            if (currentPos == servers.size())
                currentPos = 0;
            ServerEntry entry = (ServerEntry) servers.get(currentPos++);
            if (debug)
                System.out.println(dbg() + ", nRetries=" + nRetries + ", attempt to create connection to: " + entry);
            active = createConnection(entry, parameters);
            if (debug) System.out.println(dbg() + ", nRetries=" + nRetries + ", createConnection returns " + active);
            if (active == null) {
                if (!enabled)
                    break;
                nRetries++;
            }
            firstConnectAttempt = false;
        }
        if (debug) System.out.println(dbg() + ", getConnection returns " + active);
        return active;
    }

    public synchronized void invalidateConnection() {
        if (debug) System.out.println(dbg() + ", invalidateConnection, active=" + active);
        if (active != null) {
            active.close();
            active = null;
        }
    }

    public synchronized void close() {
        if (debug) System.out.println(dbg() + ", close, active=" + active);
        closed = true;
        if (active != null) {
            active.close();
            active = null;
        }
        notify();
    }

    public String toString() {
        return "Reconnector";
    }
}
