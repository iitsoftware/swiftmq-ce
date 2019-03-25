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

package com.swiftmq.mgmt.protocol;

import com.swiftmq.tools.dump.Dumpable;
import com.swiftmq.tools.dump.DumpableFactory;

public class ProtocolFactory extends DumpableFactory {
    public static final int PROTOCOL_REQ = 0;
    public static final int PROTOCOL_REP = 1;

    DumpableFactory protocolFactory = null;

    public ProtocolFactory() {
    }

    public ProtocolFactory(DumpableFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    public void setProtocolFactory(DumpableFactory protocolFactory) {
        this.protocolFactory = protocolFactory;
    }

    public Dumpable createDumpable(int dumpId) {
        Dumpable dumpable = null;

        switch (dumpId) {
            case PROTOCOL_REQ:
                dumpable = new ProtocolRequest();
                break;
            case PROTOCOL_REP:
                dumpable = new ProtocolReply();
                break;
            default:
                if (protocolFactory != null)
                    dumpable = protocolFactory.createDumpable(dumpId);
                break;
        }
        return dumpable;
    }
}
