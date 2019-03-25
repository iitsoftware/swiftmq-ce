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

import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.net.NetworkSwiftlet;

import java.util.List;
import java.util.Map;

public class IntraVMReconnector extends Reconnector {
    public IntraVMReconnector(List servers, Map parameters, boolean enabled, int maxRetries, long retryDelay, boolean debug) {
        super(servers, parameters, enabled, maxRetries, retryDelay, debug);
    }

    public boolean isIntraVM() {
        return true;
    }

    protected Connection createConnection(ServerEntry entry, Map parameters) {
        Connection connection = null;
        try {
            connection = new IntraVMConnection();
            NetworkSwiftlet networkSwiftlet = (NetworkSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$net");
            networkSwiftlet.connectIntraVMListener("sys$jms", (IntraVMConnection) connection);
        } catch (Exception e) {
            if (debug) System.out.println(toString() + " exception creating connection: " + e);
        }
        return connection;
    }

    public String toString() {
        return "IntraVMReconnector";
    }
}
