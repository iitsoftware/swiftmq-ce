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

package com.swiftmq.admin.mgmt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EndpointRegistry {
    Map endpoints = new HashMap();
    boolean closed = false;

    public EndpointRegistry() {
    }

    public synchronized void put(String routerName, Endpoint endpoint) throws EndpointRegistryClosedException {
        if (closed)
            throw new EndpointRegistryClosedException("EndpointRegistry already closed!");
        endpoints.put(routerName, endpoint);
    }

    public synchronized Endpoint get(String routerName) {
        return (Endpoint) endpoints.get(routerName);
    }

    public synchronized Endpoint remove(String routerName) {
        return (Endpoint) endpoints.remove(routerName);
    }

    public void close() {
        Map map;
        synchronized (this) {
            map = (Map) ((HashMap) endpoints).clone();
            endpoints.clear();
            closed = true;
        }
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext(); ) {
            Endpoint endpoint = (Endpoint) ((Map.Entry) iter.next()).getValue();
            endpoint.close();
        }
    }
}
