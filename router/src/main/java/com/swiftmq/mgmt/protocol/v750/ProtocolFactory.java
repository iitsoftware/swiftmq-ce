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

package com.swiftmq.mgmt.protocol.v750;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class ProtocolFactory extends DumpableFactory {
    public static final int AUTH_REQ = 100;
    public static final int AUTH_REP = 101;
    public static final int BULK_REQ = 102;
    public static final int COMMAND_REQ = 103;
    public static final int COMMAND_REP = 104;
    public static final int CONNECT_REQ = 105;
    public static final int CONNECT_REP = 106;
    public static final int ENTITYADDED_REQ = 107;
    public static final int ENTITYREMOVED_REQ = 108;
    public static final int LEASE_REQ = 109;
    public static final int PROPERTYCHANGED_REQ = 110;
    public static final int ROUTERAVAILABLE_REQ = 111;
    public static final int ROUTERUNAVAILABLE_REQ = 112;
    public static final int ROUTERCONFIG_REQ = 113;
    public static final int DISCONNECTED_REQ = 114;
    public static final int SWIFTLETADDED_REQ = 115;
    public static final int SWIFTLETREMOVED_REQ = 116;
    public static final int SETSUBSCRIPTIONFILTER_REQ = 117;
    public static final int REMOVESUBSCRIPTIONFILTER_REQ = 118;
    public static final int ENTITYLISTCLEAR_REQ = 119;

    public Dumpable createDumpable(int dumpId) {
        Dumpable dumpable = null;

        switch (dumpId) {
            case AUTH_REQ:
                dumpable = new AuthRequest();
                break;
            case AUTH_REP:
                dumpable = new AuthReply();
                break;
            case BULK_REQ:
                dumpable = new BulkRequest();
                break;
            case COMMAND_REQ:
                dumpable = new CommandRequest();
                break;
            case COMMAND_REP:
                dumpable = new CommandReply();
                break;
            case CONNECT_REQ:
                dumpable = new ConnectRequest();
                break;
            case CONNECT_REP:
                dumpable = new ConnectReply();
                break;
            case ENTITYADDED_REQ:
                dumpable = new EntityAddedRequest();
                break;
            case ENTITYREMOVED_REQ:
                dumpable = new EntityRemovedRequest();
                break;
            case LEASE_REQ:
                dumpable = new LeaseRequest();
                break;
            case PROPERTYCHANGED_REQ:
                dumpable = new PropertyChangedRequest();
                break;
            case ROUTERAVAILABLE_REQ:
                dumpable = new RouterAvailableRequest();
                break;
            case ROUTERUNAVAILABLE_REQ:
                dumpable = new RouterUnavailableRequest();
                break;
            case ROUTERCONFIG_REQ:
                dumpable = new RouterConfigRequest();
                break;
            case DISCONNECTED_REQ:
                dumpable = new DisconnectedRequest();
                break;
            case SWIFTLETADDED_REQ:
                dumpable = new SwiftletAddedRequest();
                break;
            case SWIFTLETREMOVED_REQ:
                dumpable = new SwiftletRemovedRequest();
                break;
            case SETSUBSCRIPTIONFILTER_REQ:
                dumpable = new SetSubscriptionFilterRequest();
                break;
            case REMOVESUBSCRIPTIONFILTER_REQ:
                dumpable = new RemoveSubscriptionFilterRequest();
                break;
            case ENTITYLISTCLEAR_REQ:
                dumpable = new EntityListClearRequest();
                break;
        }
        return dumpable;
    }
}
