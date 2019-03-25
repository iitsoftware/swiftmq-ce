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

package com.swiftmq.impl.routing.single.connection.stage;

import com.swiftmq.impl.routing.single.SwiftletContext;
import com.swiftmq.impl.routing.single.connection.RoutingConnection;

import java.util.ArrayList;
import java.util.List;

public class StageFactory {
    public static final String PROT_V400 = "04.00.00";
    public static final String PROT_V942 = "09.04.02";

    public static List getProtocolVersions() {
        List al = new ArrayList();
        al.add(PROT_V942);
        al.add(PROT_V400);
        return al;
    }

    public static String selectProtocol(List availableProts) {
        for (int i = 0; i < availableProts.size(); i++) {
            if (((String) availableProts.get(i)).equals(PROT_V942))
                return PROT_V942;
            if (((String) availableProts.get(i)).equals(PROT_V400))
                return PROT_V400;
        }
        return null;
    }

    public static Stage createFirstStage(SwiftletContext ctx, RoutingConnection routingConnection, String protocol) {
        Stage stage = null;
        if (protocol.equals(PROT_V400))
            stage = new com.swiftmq.impl.routing.single.connection.v400.ConnectStage(ctx, routingConnection);
        else if (protocol.equals(PROT_V942))
            stage = new com.swiftmq.impl.routing.single.connection.v942.ConnectStage(ctx, routingConnection);
        return stage;
    }
}
